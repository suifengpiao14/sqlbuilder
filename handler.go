package sqlbuilder

import (
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type CountHandler func(sql string) (count int64, err error)
type PaginationHandler func(totalSql string, listSql string, result any) (count int64, err error)
type QueryHandler func(sql string, result any) (err error)
type FirstHandler func(sql string, result any) (exists bool, err error)
type ExecHandler func(sql string) (err error)

type Handler interface {
	Exec(sql string) (err error)
	First(sql string, result any) (exists bool, err error)
	Query(sql string, result any) (err error)
	Pagination(totalSql string, listSql string, result any) (count int64, err error)
	Count(sql string) (count int64, err error)
}

type GormHandler func() *gorm.DB

func NewGormHandler(getDB func() *gorm.DB) GormHandler {
	return GormHandler(getDB)
}

func (h GormHandler) Exec(sql string) (err error) {
	return h().Exec(sql).Error
}
func (h GormHandler) First(sql string, result any) (exists bool, err error) {
	err = h().Raw(sql).First(result).Error
	exists = true
	if err != nil {
		exists = false                              // 有错误，认为不存在
		if errors.Is(err, gorm.ErrRecordNotFound) { // 明确不存在时，不返回错误
			exists = false
			err = nil
		}
	}

	if err != nil {
		return false, err
	}
	return exists, nil
}

func (h GormHandler) Query(sql string, result any) (err error) {
	return h().Raw(sql).Find(result).Error
}
func (h GormHandler) Pagination(totalSql string, listSql string, result any) (count int64, err error) {
	if err = h().Raw(totalSql).Count(&count).Error; err != nil {
		return 0, err
	}
	if count > 0 {
		if err = h().Raw(listSql).Find(result).Error; err != nil {
			return 0, err
		}
	}
	return count, nil
}
func (h GormHandler) Count(sql string) (count int64, err error) {
	err = h().Raw(sql).Count(&count).Error
	return count, err
}

func (h GormHandler) GetDB() *gorm.DB {
	return h()
}
