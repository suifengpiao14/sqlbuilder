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

// Compiler
type Compiler struct {
	handler        Handler
	defaultHandler Handler
	fields         Fields
	batchFields    []Fields
	tableName      string
	insertEvent    EventInsertTrigger
	updateEvent    EventUpdateTrigger
	deleteEvent    EventDeletedTrigger
}

func (h *Compiler) WithHandler(handler Handler) *Compiler {
	h.handler = handler
	return h
}

func (h *Compiler) WithDefaultHandler(defaultHandler Handler) *Compiler {
	h.defaultHandler = defaultHandler
	return h
}
func (h *Compiler) WithFields(fs ...*Field) *Compiler {
	h.fields = fs
	return h
}
func (h *Compiler) WithBatchFields(batchFs ...Fields) *Compiler {
	h.batchFields = batchFs
	return h
}
func (h *Compiler) WithTable(table string) *Compiler {
	h.tableName = table
	return h
}

func (h *Compiler) Handler() Handler {
	if h.handler != nil {
		return h.handler
	}
	if h.defaultHandler != nil {
		return h.defaultHandler
	}
	panic(errors.New("handler is nil"))
}
func (h *Compiler) Table() (table string) {
	if h.tableName != "" {
		return h.tableName
	}
	panic(errors.New("table name is nil"))
}
func (h *Compiler) Fields() (fs Fields) {
	if len(h.fields) > 0 {
		return h.fields
	}
	panic(errors.New("fields is nil"))
}
func (h *Compiler) BatchFields() (batchFs []Fields) {
	if len(h.batchFields) > 0 {
		return h.batchFields
	}
	panic(errors.New("batchFields is nil"))
}

func (h *Compiler) Insert() *InsertParam {
	return NewInsertBuilder(h.Table()).WithHandler(h.Handler().InsertWithLastIdHandler).WithTriggerEvent(h.insertEvent).AppendFields(h.Fields()...)
}
func (h *Compiler) InsertBatch() *BatchInsertParam {
	return NewBatchInsertBuilder(h.Table()).WithHandler(h.Handler().InsertWithLastIdHandler).WithTriggerEvent(h.insertEvent).AppendFields(h.BatchFields()...)
}

func (h *Compiler) Update() *UpdateParam {
	return NewUpdateBuilder(h.Table()).WithHandler(h.Handler().ExecWithRowsAffected).WithTriggerEvent(h.updateEvent).AppendFields(h.Fields()...)
}

func (h *Compiler) Delete() *DeleteParam {
	return NewDeleteBuilder(h.Table()).WithHandler(h.Handler().ExecWithRowsAffected).WithTriggerEvent(h.deleteEvent).AppendFields(h.Fields()...)
}
func (h *Compiler) Exists() *ExistsParam {
	return NewExistsBuilder(h.Table()).WithHandler(h.Handler().Exists).AppendFields(h.Fields()...)
}
func (h *Compiler) Count() *TotalParam {
	return NewTotalBuilder(h.Table()).WithHandler(h.Handler().Count).AppendFields(h.Fields()...)
}
func (h *Compiler) First() *FirstParam {
	return NewFirstBuilder(h.Table()).WithHandler(h.Handler().First).AppendFields(h.Fields()...)
}
func (h *Compiler) List() *ListParam {
	return NewListBuilder(h.Table()).WithHandler(h.Handler().Query).AppendFields(h.Fields()...)
}
func (h *Compiler) Pagination() *PaginationParam {
	return NewPaginationBuilder(h.Table()).WithHandler(h.Handler().Count, h.Handler().Query).AppendFields(h.Fields()...)
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
