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
	_ "github.com/go-sql-driver/mysql"
	// Register sqlite3 driver for sql.DB
	_ "github.com/mattn/go-sqlite3"
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

// GormDBMakeMysql 生成一个gorm.DB的工厂方法，该方法只会执行一次，后续调用直接返回第一次生成的db实例。该方法返回的结果需要保存到变量里面，不然还是会被重新生成。多个mysq 连接实例，可以分别调用后保存到变量
func GormDBMakeMysql(userName string, password string, host string, port int, database string, gormConfig *gorm.Config) func() *gorm.DB {
	gormDB := sync.OnceValue(func() (gormDB *gorm.DB) {
		dsn := fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=False&timeout=300s&loc=Local",
			userName,
			password,
			host,
			port,
			database,
		)
		gormDB, err := gorm.Open(mysql.Open(dsn), gormConfig)
		if err != nil {
			panic(err)
		}
		listenForExitSignal(gormDB)
		return gormDB
	})
	return gormDB
}

func listenForExitSignal(gormDB *gorm.DB) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

		sig := <-c
		log.Printf("[DB] received signal: %s, closing db connections...", sig)
		db, _ := gormDB.DB()
		if db != nil {
			db.Close()
		}
		signal.Stop(c)
	}()
}
