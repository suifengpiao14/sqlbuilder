package sqlbuilder

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// Register MySQL dialect for goqu
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"

	// Register sqlite3 dialect for goqu
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	// Register MySQL driver for sql.DB

	// Register sqlite3 driver for sql.DB
	_ "github.com/mattn/go-sqlite3"
	"github.com/suifengpiao14/sshmysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// DriverName 默认为Driver_sqlite3 驱动，可用于快速试用测试
// Deprecated: use GetDB
var DriverName = Driver_sqlite3

var GetDB func() *sql.DB = sync.OnceValue(func() (db *sql.DB) {
	db, err := sql.Open(Driver_sqlite3.String(), "sqlbuilder_example.db")
	if err != nil {
		panic(err)
	}
	return db
})

var dbHandlerPool sync.Map

/*
收集错误 1049 Unknown database 'developer_service1'
*/

func MakeDBHandler(dbConfig DBConfig) func() *sql.DB {
	dbDNS := dbConfig.DSN()
	// 读取或创建
	handlerFn, loaded := dbHandlerPool.LoadOrStore(dbDNS, sync.OnceValue(GetMysqlDBWithConfig(dbConfig)))
	handler := handlerFn.(func() *sql.DB)
	db := handler()
	// 校验连接是否有效
	if err := db.QueryRow("SELECT 1;").Err(); err != nil {
		if loaded {
			// 失效则重新建立并替换
			dbFn := GetMysqlDBWithConfig(dbConfig)
			dbHandlerPool.Store(dbDNS, dbFn)
			return dbFn
		} else {
			panic(err) // 第一次存储后链接就失败，大部分是配置问题，直接panic
		}

	}
	return handler
}

func DBHandler2Singleton(sqlDBFn func() *sql.DB) func() *sql.DB {
	return sync.OnceValue(func() *sql.DB {
		sqlDB := sqlDBFn()
		return sqlDB
	})
}

func GetMysqlDB(dsn string, applyFns ...func(db *sql.DB)) func() *sql.DB {
	return func() *sql.DB {
		sqlDB, err := sql.Open(string(Driver_mysql), dsn)
		if err != nil {
			panic(err)
		}

		for _, applyFn := range applyFns {
			applyFn(sqlDB)
		}

		listenForExitSignal(sqlDB)
		return sqlDB
	}
}

func GetMysqlDBWithConfig(dbConfig DBConfig) func() *sql.DB {
	dsn := dbConfig.DSN()
	if dbConfig.SSHConfig != nil {
		err := dbConfig.SSHConfig.RegisterNetwork(dsn)
		if err != nil {
			panic(err)
		}
	}
	dbFn := GetMysqlDB(dsn, dbConfig.DBPoolConfig.Apply)
	return dbFn
}

type DBPoolConfig struct {
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  time.Duration
}

func (poolCfg DBPoolConfig) Apply(db *sql.DB) {
	db.SetMaxOpenConns(poolCfg.MaxOpenConns)
	if poolCfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(poolCfg.MaxIdleConns)
	}
	db.SetConnMaxIdleTime(time.Duration(poolCfg.MaxIdleTime) * time.Second)
}

// DB2Gorm 将sql.DB转为gorm.DB
func DB2Gorm(sqlDBFn func() *sql.DB, gormConfig *gorm.Config) func() *gorm.DB {
	return sync.OnceValue(func() *gorm.DB {
		if gormConfig == nil {
			gormConfig = &gorm.Config{}
		}
		sqlDB := sqlDBFn()
		driver := DetectDriver(sqlDB)
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

func DetectDriver(db *sql.DB) string {
	var version string
	// 尝试执行 SQLite 语句
	if err := db.QueryRow("SELECT sqlite_version()").Scan(&version); err == nil {
		return Driver_sqlite3.String()
	}
	// 尝试 MySQL
	if err := db.QueryRow("SELECT VERSION()").Scan(&version); err == nil {
		return Driver_mysql.String()
	}
	return "unknown"
}
func detectDriverTx(tx *sql.Tx) string {
	var version string
	// 尝试执行 SQLite 语句
	if err := tx.QueryRow("SELECT sqlite_version()").Scan(&version); err == nil {
		return Driver_sqlite3.String()
	}
	// 尝试 MySQL
	if err := tx.QueryRow("SELECT VERSION()").Scan(&version); err == nil {
		return Driver_mysql.String()
	}
	return "unknown"
}

type DBConfig struct {
	UserName     string
	Password     string
	Host         string
	Port         int
	DatabaseName string
	QueryParams  string
	SSHConfig    *sshmysql.SSHConfig
	DBPoolConfig DBPoolConfig
}

func (dbConfig DBConfig) DSN() string {
	if dbConfig.QueryParams == "" {
		dbConfig.QueryParams = "charset=utf8mb4&parseTime=False&timeout=300s&loc=Local"
	}
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?%s",
		dbConfig.UserName,
		dbConfig.Password,
		dbConfig.Host,
		dbConfig.Port,
		dbConfig.DatabaseName,
		dbConfig.QueryParams,
	)
}

func listenForExitSignal(db *sql.DB) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

		sig := <-c
		log.Printf("[DB] received signal: %s, closing db connections...", sig)
		if db != nil {
			db.Close()
		}
		signal.Stop(c)
	}()
}
