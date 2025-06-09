package sqlbuilder

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// DriverName 默认为Driver_sqlite3 驱动，可用于快速试用测试
var DriverName = Driver_sqlite3

var GetDB func() *sql.DB = sync.OnceValue(func() (db *sql.DB) {
	db, err := sql.Open(Driver_sqlite3.String(), "sqlbuilder_example.db")
	if err != nil {
		panic(err)
	}
	return db
})

var GormDB func() *gorm.DB = sync.OnceValue(func() (db *gorm.DB) {
	var dialector gorm.Dialector
	var err error
	sqlDB := GetDB()
	switch DriverName {
	case Driver_mysql:
		dialector = mysql.New(mysql.Config{Conn: sqlDB})
	case Driver_sqlite3:
		dialector = sqlite.Dialector{Conn: sqlDB}
	default:
		err = errors.Errorf("unsupported driverName :%s", DriverName)
		panic(err)
	}
	db, err = gorm.Open(dialector, &gorm.Config{})
	if err != nil {
		panic(err)
	}
	return db
})

func NewGormDBExample(userName string, password string, host string, port int, database string) func() *gorm.DB {
	gormDB := sync.OnceValue(func() (db *gorm.DB) {
		dsn := fmt.Sprintf(
			"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=False&timeout=300s&loc=Local",
			userName,
			password,
			host,
			port,
			database,
		)
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			panic(err)
		}
		return db
	})
	return gormDB
}
