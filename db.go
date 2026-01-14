package sqlbuilder

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// 注册goqu方言
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"

	// 注册SQL驱动
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"

	// SSH MySQL支持
	"github.com/suifengpiao14/sshmysql"
	// GORM驱动
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// -------------------------- 枚举定义（规范驱动类型） --------------------------
// DriverType 数据库驱动类型枚举
type DriverType string

const (
	DriverMySQL  DriverType = "mysql"   // MySQL驱动
	DriverSQLite DriverType = "sqlite3" // SQLite驱动
)

// String 返回驱动类型字符串
func (d DriverType) String() string {
	return string(d)
}

// -------------------------- 配置结构体（整合&规范） --------------------------
// DBPoolConfig 数据库连接池配置
type DBPoolConfig struct {
	MaxOpenConns    int           // 最大打开连接数
	MaxIdleConns    int           // 最大空闲连接数
	ConnMaxIdleTime time.Duration // 连接最大空闲时间（秒）
	ConnMaxLifetime time.Duration // 连接最大存活时间（秒）
}

// ReconnectConfig 重连配置
type ReconnectConfig struct {
	MaxRetries int           // 最大重试次数
	RetryDelay time.Duration // 重试间隔（毫秒）
}

// DBConfig 数据库核心配置
type DBConfig struct {
	DriverType      DriverType          // 驱动类型（必填）
	DSN             string              // 直接指定DSN（优先级高于下面的字段）
	UserName        string              // MySQL-用户名
	Password        string              // MySQL-密码
	Host            string              // MySQL-主机
	Port            int                 // MySQL-端口
	DatabaseName    string              // MySQL-数据库名/SQLite-文件路径
	QueryParams     string              // MySQL-连接参数
	SSHConfig       *sshmysql.SSHConfig // MySQL-SSH隧道配置
	DBPoolConfig    DBPoolConfig        // 连接池配置
	ReconnectConfig ReconnectConfig     // 重连配置
}

// 默认配置
var (
	defaultPoolConfig = DBPoolConfig{
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 30 * time.Second,
		ConnMaxLifetime: 1 * time.Hour,
	}

	defaultReconnectConfig = ReconnectConfig{
		MaxRetries: 3,
		RetryDelay: 500 * time.Millisecond,
	}

	defaultGormConfig = &gorm.Config{
		Logger: logger.Default.LogMode(logger.Error), // 默认只打印错误日志
	}
)

// -------------------------- 核心管理器（单实例&重连核心） --------------------------
// DBManager 数据库管理器（单例，管理所有数据库实例）
type DBManager struct {
	sync.RWMutex
	sqlDBs     map[string]*sql.DB  // 缓存sql.DB实例（key: 驱动+DSN）
	gormDBs    map[string]*gorm.DB // 缓存gorm.DB实例（key: 驱动+DSN）
	signalOnce sync.Once           // 确保信号监听只执行一次
	closed     bool                // 管理器是否已关闭
}

// 全局DB管理器单例
var globalDBManager = &DBManager{
	sqlDBs:  make(map[string]*sql.DB),
	gormDBs: make(map[string]*gorm.DB),
}

// -------------------------- 配置校验&辅助方法 --------------------------
// validate 校验DBConfig合法性
func (c *DBConfig) validate() error {
	if c.DriverType == "" {
		return errors.New("驱动类型DriverType不能为空")
	}

	// 补全默认配置
	if c.DBPoolConfig.MaxOpenConns == 0 {
		c.DBPoolConfig = defaultPoolConfig
	}
	if c.ReconnectConfig.MaxRetries == 0 {
		c.ReconnectConfig = defaultReconnectConfig
	}

	// MySQL必须指定DSN或账号密码等信息
	if c.DriverType == DriverMySQL && c.DSN == "" {
		if c.UserName == "" || c.Host == "" || c.DatabaseName == "" {
			return errors.New("MySQL驱动必须指定DSN或UserName+Host+DatabaseName")
		}
		if c.Port == 0 {
			c.Port = 3306 // 默认MySQL端口
		}
		if c.QueryParams == "" {
			c.QueryParams = "charset=utf8mb4&parseTime=False&timeout=300s&loc=Local"
		}
	}

	// SQLite必须指定DSN或文件路径
	if c.DriverType == DriverSQLite && c.DSN == "" && c.DatabaseName == "" {
		return errors.New("SQLite驱动必须指定DSN或DatabaseName（文件路径）")
	}

	return nil
}

// buildDSN 构建DSN（根据驱动类型）
func (c *DBConfig) buildDSN() (string, error) {
	if c.DSN != "" {
		return c.DSN, nil
	}

	switch c.DriverType {
	case DriverMySQL:
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s",
			c.UserName, c.Password, c.Host, c.Port, c.DatabaseName, c.QueryParams), nil
	case DriverSQLite:
		return c.DatabaseName, nil // SQLite的DSN就是文件路径
	default:
		return "", fmt.Errorf("不支持的驱动类型：%s", c.DriverType)
	}
}

// applyPoolConfig 应用连接池配置到sql.DB
func applyPoolConfig(db *sql.DB, cfg DBPoolConfig) {
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
}

// registerSSH SSH隧道注册（仅MySQL）
func registerSSH(cfg *DBConfig, dsn string) (string, error) {
	if cfg.DriverType != DriverMySQL || cfg.SSHConfig == nil {
		return dsn, nil
	}
	if err := cfg.SSHConfig.RegisterNetwork(dsn); err != nil {
		return "", fmt.Errorf("SSH隧道注册失败：%w", err)
	}
	return dsn, nil
}

func GetSQLDB(cfg DBConfig) func() *sql.DB {
	return func() *sql.DB {
		db, err := getSQLDB(cfg)
		if err != nil {
			panic(err)
		}
		return db
	}
}

// -------------------------- 核心方法（单实例&重连） --------------------------
// getSQLDB 获取sql.DB实例（单例，自动重连）
func getSQLDB(cfg DBConfig) (*sql.DB, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("配置校验失败：%w", err)
	}

	// 构建唯一key（驱动+DSN）
	dsn, err := cfg.buildDSN()
	if err != nil {
		return nil, fmt.Errorf("构建DSN失败：%w", err)
	}
	key := fmt.Sprintf("%s_%s", cfg.DriverType, dsn)

	// 1. 尝试从缓存获取
	globalDBManager.RLock()
	db, exists := globalDBManager.sqlDBs[key]
	globalDBManager.RUnlock()

	// 2. 缓存存在且连接有效，直接返回
	if exists && !globalDBManager.closed {
		if err := pingWithRetry(db, cfg.ReconnectConfig); err == nil {
			return db, nil
		}
		log.Printf("数据库连接失效（key：%s），准备重建连接", key)
	}

	// 3. 缓存不存在/连接失效，创建新连接（加写锁）
	globalDBManager.Lock()
	defer globalDBManager.Unlock()

	// 双重检查（避免并发创建）
	if exists && !globalDBManager.closed {
		if err := pingWithRetry(db, cfg.ReconnectConfig); err == nil {
			return db, nil
		}
		// 关闭失效连接
		if err := db.Close(); err != nil {
			log.Printf("关闭失效连接失败（key：%s）：%v", key, err)
		}
	}

	// 4. 创建新连接
	newDB, err := createSQLDB(cfg, dsn)
	if err != nil {
		return nil, fmt.Errorf("创建数据库连接失败：%w", err)
	}

	// 5. 缓存新连接
	globalDBManager.sqlDBs[key] = newDB
	log.Printf("成功创建/重建数据库连接（key：%s）", key)

	// 6. 全局信号监听（只执行一次）
	globalDBManager.signalOnce.Do(listenExitSignal)

	return newDB, nil
}

func GetGormDB(cfg DBConfig, gormCfg ...*gorm.Config) func() *gorm.DB {
	return func() *gorm.DB {
		db, err := getGormDB(cfg, gormCfg...)
		if err != nil {
			panic(err)
		}
		return db
	}
}

// getGormDB 获取gorm.DB实例（单例，自动重连）
func getGormDB(cfg DBConfig, gormCfg ...*gorm.Config) (*gorm.DB, error) {
	// 1. 获取sql.DB实例
	sqlDB, err := getSQLDB(cfg)
	if err != nil {
		return nil, fmt.Errorf("获取sql.DB失败：%w", err)
	}

	// 2. 构建唯一key
	dsn, _ := cfg.buildDSN()
	key := fmt.Sprintf("%s_%s_gorm", cfg.DriverType, dsn)

	// 3. 尝试从缓存获取
	globalDBManager.RLock()
	gormDB, exists := globalDBManager.gormDBs[key]
	globalDBManager.RUnlock()

	// 4. 缓存存在且连接有效，直接返回
	if exists && !globalDBManager.closed {
		if sqlDB, err := gormDB.DB(); err == nil && pingWithRetry(sqlDB, cfg.ReconnectConfig) == nil {
			return gormDB, nil
		}
		log.Printf("GORM连接失效（key：%s），准备重建连接", key)
	}

	// 5. 创建新GORM连接（加写锁）
	globalDBManager.Lock()
	defer globalDBManager.Unlock()

	// 双重检查
	if exists && !globalDBManager.closed {
		if sqlDB, err := gormDB.DB(); err == nil && pingWithRetry(sqlDB, cfg.ReconnectConfig) == nil {
			return gormDB, nil
		}
	}

	// 6. 构建GORM配置
	config := defaultGormConfig
	if len(gormCfg) > 0 && gormCfg[0] != nil {
		config = gormCfg[0]
	}

	// 7. 创建GORM连接
	var dialector gorm.Dialector
	switch cfg.DriverType {
	case DriverMySQL:
		dialector = mysql.New(mysql.Config{Conn: sqlDB})
	case DriverSQLite:
		dialector = sqlite.New(sqlite.Config{Conn: sqlDB})
	default:
		return nil, fmt.Errorf("不支持的GORM驱动类型：%s", cfg.DriverType)
	}

	newGormDB, err := gorm.Open(dialector, config)
	if err != nil {
		return nil, fmt.Errorf("创建GORM连接失败：%w", err)
	}

	// 8. 缓存GORM连接
	globalDBManager.gormDBs[key] = newGormDB
	log.Printf("成功创建/重建GORM连接（key：%s）", key)

	return newGormDB, nil
}

// Close 关闭所有数据库连接
func Close() {
	globalDBManager.Lock()
	defer globalDBManager.Unlock()

	if globalDBManager.closed {
		return
	}

	// 关闭所有sql.DB
	for key, db := range globalDBManager.sqlDBs {
		if err := db.Close(); err != nil {
			log.Printf("关闭sql.DB失败（key：%s）：%v", key, err)
		} else {
			log.Printf("成功关闭sql.DB（key：%s）", key)
		}
		delete(globalDBManager.sqlDBs, key)
	}

	// 关闭所有gorm.DB（实际还是关闭底层sql.DB）
	for key, gormDB := range globalDBManager.gormDBs {
		if sqlDB, err := gormDB.DB(); err == nil {
			if err := sqlDB.Close(); err != nil {
				log.Printf("关闭gorm.DB失败（key：%s）：%v", key, err)
			} else {
				log.Printf("成功关闭gorm.DB（key：%s）", key)
			}
		}
		delete(globalDBManager.gormDBs, key)
	}

	globalDBManager.closed = true
	log.Println("所有数据库连接已关闭")
}

// -------------------------- 内部工具方法 --------------------------
// createSQLDB 创建新的sql.DB实例（包含SSH注册、连接池配置）
func createSQLDB(cfg DBConfig, dsn string) (*sql.DB, error) {
	// 1. SSH隧道注册（仅MySQL）
	dsn, err := registerSSH(&cfg, dsn)
	if err != nil {
		return nil, fmt.Errorf("SSH注册失败：%w", err)
	}

	// 2. 打开数据库连接
	db, err := sql.Open(cfg.DriverType.String(), dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open失败：%w", err)
	}

	// 3. 应用连接池配置
	applyPoolConfig(db, cfg.DBPoolConfig)

	// 4. 校验连接有效性（带重试）
	if err := pingWithRetry(db, cfg.ReconnectConfig); err != nil {
		db.Close()
		return nil, fmt.Errorf("连接校验失败：%w", err)
	}

	return db, nil
}

// pingWithRetry 带重试的Ping操作
func pingWithRetry(db *sql.DB, cfg ReconnectConfig) error {
	var err error
	for i := 0; i < cfg.MaxRetries; i++ {
		if err = db.Ping(); err == nil {
			return nil
		}
		log.Printf("数据库Ping失败（重试%d/%d）：%v", i+1, cfg.MaxRetries, err)
		time.Sleep(cfg.RetryDelay)
	}
	return fmt.Errorf("超出最大重试次数：%w", err)
}

// listenExitSignal 监听退出信号（全局只执行一次）
func listenExitSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-c
		log.Printf("收到退出信号：%s，开始关闭数据库连接...", sig)
		Close()
		signal.Stop(c)
		close(c)
	}()
}

// -------------------------- 兼容原有功能（可选） --------------------------
// GetDefaultSQLiteDB 获取默认SQLite实例（兼容原有GetDB）
func GetDefaultSQLiteDB() (*sql.DB, error) {
	cfg := DBConfig{
		DriverType:   DriverSQLite,
		DatabaseName: "sqlbuilder_example.db",
	}
	return getSQLDB(cfg)
}

// 兼容原有GetDB（建议逐步替换为GetDefaultSQLiteDB）
var GetDB = func() *sql.DB {
	db, err := GetDefaultSQLiteDB()
	if err != nil {
		log.Panicf("获取默认SQLite连接失败：%v", err)
	}
	return db
}
