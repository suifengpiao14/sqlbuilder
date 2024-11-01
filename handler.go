package sqlbuilder

import (
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type EventInsertTrigger func(lastInsertId uint64, rowsAffected int64) (err error) // 新增事件触发器
type EventUpdateTrigger func(rowsAffected int64) (err error)                      // 更新事件触发器
type EventDeletedTrigger EventUpdateTrigger                                       // 删除事件触发器

type CountHandler func(sql string) (count int64, err error)
type QueryHandler func(sql string, result any) (err error)
type FirstHandler func(sql string, result any) (exists bool, err error)
type ExecHandler func(sql string) (err error)
type ExistsHandler func(sql string) (exists bool, err error)
type ExecWithRowsAffectedHandler func(sql string) (rowsAffected int64, err error)
type InsertWithLastIdHandler func(sql string) (lastInsertId uint64, rowsAffected int64, err error)

func WarpUpdateWithEventTrigger(updateHander ExecWithRowsAffectedHandler, eventUpdateTrigger EventUpdateTrigger) ExecWithRowsAffectedHandler {
	return func(sql string) (rowsAffected int64, err error) {
		rowsAffected, err = updateHander(sql)
		if err != nil {
			return
		}

		if eventUpdateTrigger != nil {
			err = eventUpdateTrigger(rowsAffected)
			if err != nil {
				return rowsAffected, err
			}
		}
		return rowsAffected, nil
	}
}

func WarpInsertWithEventTrigger(insertHander InsertWithLastIdHandler, eventInsertTrigger EventInsertTrigger) InsertWithLastIdHandler {
	return func(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
		lastInsertId, rowsAffected, err = insertHander(sql)
		if err != nil {
			return
		}

		if eventInsertTrigger != nil {
			err = eventInsertTrigger(lastInsertId, rowsAffected)
			if err != nil {
				return lastInsertId, rowsAffected, err
			}
		}
		return lastInsertId, rowsAffected, nil
	}
}

// WithHandler 附加特定的执行器，在实现事务操作时使用
type WithHandler struct {
	handler Handler
}

func (h *WithHandler) WithHandler(handler Handler) {
	h.handler = handler
}

func (h *WithHandler) Handler() Handler {
	return h.handler
}

func (h *WithHandler) HandlerWithDefault(defaultHandler Handler) Handler {
	if h.handler != nil {
		return h.handler
	}
	return defaultHandler
}

type Handler interface {
	Exec(sql string) (err error)
	ExecWithRowsAffected(sql string) (rowsAffected int64, err error)
	InsertWithLastIdHandler(sql string) (lastInsertId uint64, rowsAffected int64, err error)
	First(sql string, result any) (exists bool, err error)
	Query(sql string, result any) (err error)
	Count(sql string) (count int64, err error)
	Exists(sql string) (exists bool, err error)
}

type GormHandler func() *gorm.DB

func NewGormHandler(getDB func() *gorm.DB) Handler {
	return GormHandler(getDB)
}

func (h GormHandler) Exec(sql string) (err error) {
	return h().Exec(sql).Error
}
func (h GormHandler) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
	tx := h().Exec(sql)
	return tx.RowsAffected, tx.Error
}

func (h GormHandler) Exists(sql string) (exists bool, err error) {
	result := make([]any, 0)
	err = h.Query(sql, &result)
	if err != nil {
		return false, err
	}
	exists = len(result) > 0
	return exists, nil
}
func (h GormHandler) InsertWithLastIdHandler(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
	h().Transaction(func(tx *gorm.DB) error {
		err = tx.Exec(sql).Error
		if err != nil {
			return err
		}
		rowsAffected = tx.RowsAffected
		switch tx.Dialector.Name() {
		case "mysql":
			err = tx.Raw("SELECT LAST_INSERT_ID()").Scan(&lastInsertId).Error
			if err != nil {
				return err
			}
		default:
			err = errors.New("not support last insert id")
		}
		return nil
	})

	return lastInsertId, rowsAffected, err
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

func (h GormHandler) Count(sql string) (count int64, err error) {
	err = h().Raw(sql).Count(&count).Error
	return count, err
}

func (h GormHandler) GetDB() *gorm.DB {
	return h()
}
