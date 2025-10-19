package sqlbuilder

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	// Register MySQL dialect for goqu
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	// Register sqlite3 dialect for goqu
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	// Register MySQL driver for sql.DB

	// Register sqlite3 driver for sql.DB
	_ "github.com/mattn/go-sqlite3"
	"github.com/suifengpiao14/sshmysql"
	"gorm.io/driver/mysql"
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

func DBHandler2Singleton(sqlDBFn func() *sql.DB) func() *sql.DB {
	return sync.OnceValue(func() *sql.DB {
		sqlDB := sqlDBFn()
		return sqlDB
	})
}

func GetMysqlDB(dsn string) func() *sql.DB {
	return func() *sql.DB {
		sqlDB, err := sql.Open(string(Driver_mysql), dsn)
		if err != nil {
			panic(err)
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
	return GetMysqlDB(dsn)
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
			dialector = mysql.New(mysql.Config{Conn: sqlDB})
		case Driver_sqlite3.String():
			dialector = sqlite.New(sqlite.Config{Conn: sqlDB})
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

type DBConfig struct {
	UserName     string
	Password     string
	Host         string
	Port         int
	DatabaseName string
	QueryParams  string
	SSHConfig    *sshmysql.SSHConfig
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
