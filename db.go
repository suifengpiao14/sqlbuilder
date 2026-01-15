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
	_ "github.com/mattn/go-sqlite3"

	// SSH MySQL支持
	"github.com/suifengpiao14/sshmysql"
	// GORM驱动
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
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

// DBConfig 数据库核心配置
type DBConfig struct {
	DriverType   DriverType          // 驱动类型（必填）
	DSN          string              // 直接指定DSN（优先级高于下面的字段）
	UserName     string              // MySQL-用户名
	Password     string              // MySQL-密码
	Host         string              // MySQL-主机
	Port         int                 // MySQL-端口
	DatabaseName string              // MySQL-数据库名/SQLite-文件路径
	QueryParams  string              // MySQL-连接参数
	SSHConfig    *sshmysql.SSHConfig // MySQL-SSH隧道配置
	DBPoolConfig DBPoolConfig        // 连接池配置
}

// 默认配置
var (
	defaultPoolConfig = DBPoolConfig{
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 30 * time.Second,
		ConnMaxLifetime: 1 * time.Hour,
	}
)

// -------------------------- 核心管理器（单实例&重连核心） --------------------------
// DBManager 数据库管理器（单例，管理所有数据库实例）
type DBManager struct {
	sync.RWMutex
	sqlDBs     map[string]*sql.DB // 缓存sql.DB实例（key: 驱动+DSN）
	signalOnce sync.Once          // 确保信号监听只执行一次
	closed     bool               // 管理器是否已关闭
}

// -------------------------- 配置校验&辅助方法 --------------------------
func (c *DBConfig) Format() *DBConfig {
	// 补全默认配置
	if c.DBPoolConfig.MaxOpenConns == 0 {
		c.DBPoolConfig = defaultPoolConfig
	}
	if c.DriverType == DriverMySQL && c.DSN == "" {
		if c.Port == 0 {
			c.Port = 3306 // 默认MySQL端口
		}
		if c.QueryParams == "" {
			c.QueryParams = "charset=utf8mb4&parseTime=False&timeout=300s&loc=Local"
		}
	}
	return c

}

// validate 校验DBConfig合法性
func (c *DBConfig) validate() error {
	if c.DriverType == "" {
		return errors.New("驱动类型DriverType不能为空")
	}

	// MySQL必须指定DSN或账号密码等信息
	if c.DriverType == DriverMySQL && c.DSN == "" {
		if c.UserName == "" || c.Host == "" || c.DatabaseName == "" {
			return errors.New("MySQL驱动必须指定DSN或UserName+Host+DatabaseName")
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

// -------------------------- 核心方法（单实例&重连） --------------------------
// getSQLDB 获取sql.DB实例（单例，自动重连）
func getSQLDB(cfg DBConfig) (*sql.DB, error) {
	// 1. 格式化配置
	cfg = *cfg.Format()
	// 2. 校验配置合法性
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	// 3. 构建唯一key（驱动+DSN）
	dsn, err := cfg.buildDSN()
	if err != nil {
		return nil, err
	}
	// 4. SSH隧道注册（仅MySQL）
	if cfg.SSHConfig != nil {
		err = cfg.SSHConfig.RegisterNetwork(dsn)
		if err != nil {
			return nil, err
		}
	}
	// 5. 打开数据库连接
	db, err := sql.Open(cfg.DriverType.String(), dsn)
	if err != nil {
		return nil, err
	}
	// 6. 应用连接池配置
	applyPoolConfig(db, cfg.DBPoolConfig)
	// 7. 全局信号监听（只执行一次）
	globalDBManager.signalOnce.Do(listenExitSignal)
	return db, nil
}

// DB2Gorm 将sql.DB转为gorm.DB
func DB2Gorm(sqlDBFn func() *sql.DB, gormConfig *gorm.Config) func() *gorm.DB {
	return sync.OnceValue(func() *gorm.DB {
		if gormConfig == nil {
			gormConfig = &gorm.Config{}
		}
		sqlDB := sqlDBFn()
		driver := detectDriver(sqlDB)
		var dialector gorm.Dialector
		switch driver {
		case Driver_mysql.String():
			dialector = gormmysql.New(gormmysql.Config{Conn: sqlDB})
		case Driver_sqlite3.String():
			dialector = sqlite.New(sqlite.Config{Conn: sqlDB})
		default:
			panic("unsupported driver")
		}

		gormDB, err := gorm.Open(dialector, gormConfig)
		if err != nil {
			panic(err)
		}
		return gormDB
	})
}

// listenExitSignal 监听退出信号（全局只执行一次）
func listenExitSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-c
		log.Printf("收到退出信号：%s，开始关闭数据库连接...", sig)
		globalDBManager.closeDB()
		signal.Stop(c)
		close(c)
	}()
}

// 全局DB管理器单例
var globalDBManager = &DBManager{
	sqlDBs: make(map[string]*sql.DB),
}

// closeDB 关闭所有数据库连接
func (m *DBManager) closeDB() {
	m.Lock()
	defer m.Unlock()

	if m.closed {
		return
	}
	// 关闭所有sql.DB
	for key, db := range m.sqlDBs {
		if err := db.Close(); err != nil {
			log.Printf("关闭sql.DB失败（key：%s）：%v", key, err)
		} else {
			log.Printf("成功关闭sql.DB（key：%s）", key)
		}
		delete(m.sqlDBs, key)
	}
	m.closed = true
	log.Println("所有数据库连接已关闭")
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

func GetGormDB(cfg DBConfig, gormConfig *gorm.Config) func() *gorm.DB {
	return DB2Gorm(GetSQLDB(cfg), gormConfig)
}

// 兼容原有GetDB（建议逐步替换为GetDefaultSQLiteDB）
var GetDB = func() *sql.DB {
	db, err := getSQLDB(SQliteConfigExample)
	if err != nil {
		log.Panicf("获取默认SQLite连接失败：%v", err)
	}
	return db
}

var SQliteConfigExample = DBConfig{
	DriverType:   DriverSQLite,
	DatabaseName: "sqlbuilder_example.db",
}
