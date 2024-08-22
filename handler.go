package sqlbuilder

import (
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

type GormHandler struct {
	getDB func() *gorm.DB
}

func NewGormHandler(getDB func() *gorm.DB) Handler {
	return &GormHandler{getDB: getDB}
}

func (h *GormHandler) Exec(sql string) (err error) {
	return h.getDB().Exec(sql).Error
}
func (h *GormHandler) First(sql string, result any) (exists bool, err error) {
	return h.getDB().First(result, sql).RowsAffected > 0, nil
}

func (h *GormHandler) Query(sql string, result any) (err error) {
	return h.getDB().Raw(sql).Find(result).Error
}
func (h *GormHandler) Pagination(totalSql string, listSql string, result any) (count int64, err error) {
	if err = h.getDB().Raw(totalSql).Count(&count).Error; err != nil {
		return 0, err
	}
	if count > 0 {
		if err = h.getDB().Raw(listSql).Find(result).Error; err != nil {
			return 0, err
		}
	}
	return count, nil
}
func (h *GormHandler) Count(sql string) (count int64, err error) {
	err = h.getDB().Raw(sql).Count(&count).Error
	return count, err
}

func (h *GormHandler) GetDB() *gorm.DB {
	return h.getDB()
}
