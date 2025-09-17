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
// Deprecated: use GormDBForSqlite3 or GormDBForMysql
var DriverName = Driver_sqlite3

// Deprecated: use GormDBForSqlite3
var GetDB func() *sql.DB = sync.OnceValue(func() (db *sql.DB) {
	db, err := sql.Open(Driver_sqlite3.String(), "sqlbuilder_example.db")
	if err != nil {
		panic(err)
	}
	return db
})

var GormDBForSqlite3 func() *gorm.DB = sync.OnceValue(func() (db *gorm.DB) {
	var dialector gorm.Dialector
	var err error
	sqlDB, err := sql.Open(Driver_sqlite3.String(), "sqlbuilder_example.db")
	if err != nil {
		panic(err)
	}
	dialector = sqlite.Dialector{Conn: sqlDB}
	db, err = gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		panic(err)
	}
	return db
})

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

func MakeMysqlDB(dsn string) func() *sql.DB {
	return sync.OnceValue(func() *sql.DB {
		sqlDB, err := sql.Open(string(Driver_mysql), dsn)
		if err != nil {
			panic(err)
		}
		listenForExitSignal(sqlDB)
		return sqlDB
	})
}
func MakeMysqlDBWithConfig(dbConfig DBConfig) func() *sql.DB {
	return sync.OnceValue(func() *sql.DB {
		dsn := dbConfig.DSN()
		if dbConfig.SSHConfig != nil {
			err := dbConfig.SSHConfig.RegisterNetwork(dsn)
			if err != nil {
				panic(err)
			}
		}
		return MakeMysqlDB(dsn)()
	})
}

func DB2Gorm(sqlDBFn func() *sql.DB, gormConfig *gorm.Config) func() *gorm.DB {
	return sync.OnceValue(func() *gorm.DB {
		sqlDB := sqlDBFn()
		dialector := mysql.New(mysql.Config{Conn: sqlDB})
		gormDB, err := gorm.Open(dialector, gormConfig)
		if err != nil {
			panic(err)
		}
		return gormDB
	})
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
