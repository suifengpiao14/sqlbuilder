package sqlbuilder

import (
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/suifengpiao14/cache"
	"golang.org/x/sync/singleflight"
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

type CompilerConfig struct {
	Table       string
	Handler     Handler
	FieldsApply ApplyFn
}

func (c CompilerConfig) WithTableIgnore(table string) CompilerConfig {
	if c.Table == "" { // 为空时才增加,即优先级低，用于设置默认值
		c.Table = table
	}
	return c
}
func (c CompilerConfig) WithHandlerIgnore(handler Handler) CompilerConfig {
	if c.Handler == nil { // 为空时才增加,即优先级低，用于设置默认值
		c.Handler = handler
	}
	return c
}

// CopyHandler 常用于传递handler
func (c CompilerConfig) CopyHandler() CompilerConfig {
	cp := CompilerConfig{
		Handler: c.Handler,
	}
	return cp

}

// CopyTableHandler 常用于传递handler和table
func (c CompilerConfig) CopyTableHandler() CompilerConfig {
	cp := CompilerConfig{
		Table:   c.Table,
		Handler: c.Handler,
	}
	return cp
}

// Compiler
type Compiler struct {
	handler     Handler
	tableName   string
	fields      Fields
	batchFields []Fields
	insertEvent EventInsertTrigger
	updateEvent EventUpdateTrigger
	deleteEvent EventDeletedTrigger
}

func NewCompiler(cfg CompilerConfig, fs ...*Field) Compiler {
	if len(fs) == 0 {
		panic(errors.New("sqlbuilder: Compiler need fields"))
	}
	c := Compiler{tableName: cfg.Table, handler: cfg.Handler, fields: fs}
	c = c.Apply(cfg)
	return c
}

func (h Compiler) WithHandler(handler Handler) Compiler {
	if handler != nil { // 增加空判断，方便使用方转入nil情况
		h.handler = handler
	}
	return h
}

func (h Compiler) Apply(cfg CompilerConfig) Compiler {
	if cfg.Table != "" { // 增加空判断，方便使用方转入nil情况
		h.tableName = cfg.Table
	}
	if cfg.Handler != nil { // 增加空判断，方便使用方转入nil情况
		h.handler = cfg.Handler
	}
	if cfg.FieldsApply != nil { // 应用列修改
		h.fields.Apply(cfg.FieldsApply)
		for i := range h.batchFields {
			h.batchFields[i].Apply(cfg.FieldsApply)
		}
	}

	return h
}

func (h Compiler) WithFields(fs ...*Field) Compiler {
	h.fields = fs
	return h
}
func (h Compiler) WithInsertEvent(insertEvent EventInsertTrigger) Compiler {
	h.insertEvent = insertEvent
	return h
}
func (h Compiler) WithUpdateEvent(updateEvent EventUpdateTrigger) Compiler {
	h.updateEvent = updateEvent
	return h
}
func (h Compiler) WithDeleteEvent(deleteEvent EventDeletedTrigger) Compiler {
	h.deleteEvent = deleteEvent
	return h
}

func (h Compiler) WithBatchFields(batchFs ...Fields) Compiler {
	h.batchFields = batchFs
	return h
}
func (h Compiler) WithTable(table string) Compiler {
	h.tableName = table
	return h
}

func (h Compiler) Handler() Handler {
	if h.handler != nil {
		return h.handler
	}
	panic(errors.New("handler is nil"))
}
func (h Compiler) Table() (table string) {
	if h.tableName != "" {
		return h.tableName
	}
	panic(errors.New("table name is nil"))
}
func (h Compiler) Fields() (fs Fields) {
	if len(h.fields) > 0 {
		return h.fields
	}
	panic(errors.New("fields is nil"))
}
func (h Compiler) BatchFields() (batchFs []Fields) {
	if len(h.batchFields) > 0 {
		return h.batchFields
	}
	panic(errors.New("batchFields is nil"))
}

func (h Compiler) Insert() *InsertParam {
	return NewInsertBuilder(h.Table()).WithHandler(h.Handler().InsertWithLastIdHandler).WithTriggerEvent(h.insertEvent).AppendFields(h.Fields()...)
}
func (h Compiler) InsertBatch() *BatchInsertParam {
	return NewBatchInsertBuilder(h.Table()).WithHandler(h.Handler().InsertWithLastIdHandler).WithTriggerEvent(h.insertEvent).AppendFields(h.BatchFields()...)
}

func (h Compiler) Update() *UpdateParam {
	return NewUpdateBuilder(h.Table()).WithHandler(h.Handler().ExecWithRowsAffected).WithTriggerEvent(h.updateEvent).AppendFields(h.Fields()...)
}

func (h Compiler) Delete() *DeleteParam {
	return NewDeleteBuilder(h.Table()).WithHandler(h.Handler().ExecWithRowsAffected).WithTriggerEvent(h.deleteEvent).AppendFields(h.Fields()...)
}
func (h Compiler) Exists() *ExistsParam {
	return NewExistsBuilder(h.Table()).WithHandler(h.Handler().Exists).AppendFields(h.Fields()...)
}
func (h Compiler) Count() *TotalParam {
	return NewTotalBuilder(h.Table()).WithHandler(h.Handler().Count).AppendFields(h.Fields()...)
}
func (h Compiler) First() *FirstParam {
	return NewFirstBuilder(h.Table()).WithHandler(h.Handler().First).AppendFields(h.Fields()...)
}
func (h Compiler) List() *ListParam {
	return NewListBuilder(h.Table()).WithHandler(h.Handler().Query).AppendFields(h.Fields()...)
}
func (h Compiler) Pagination() *PaginationParam {
	return NewPaginationBuilder(h.Table()).WithHandler(h.Handler().Count, h.Handler().Query).AppendFields(h.Fields()...)
}

func (h Compiler) Set() *SetParam {
	return NewSetBuilder(h.Table()).WithHandler(h.Handler().Exists, h.Handler().InsertWithLastIdHandler, h.Handler().ExecWithRowsAffected).AppendFields(h.Fields()...)
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

func WithCacheSingleflightHandler(handler Handler, withCache bool, withSingleflight bool) Handler {
	if withCache {
		handler = WithCache(handler)
	}
	if withSingleflight {
		handler = WithSingleflight(handler)
	}
	return handler
}

type _DbExecResult struct {
	data         any
	rowsAffected int64
	lastInsertId uint64
	exists       bool
}

type _HandlerSingleflight struct {
	handler Handler
	group   *singleflight.Group
}

func WithSingleflight(handler Handler) Handler {
	return &_HandlerSingleflight{
		handler: handler,
		group:   &singleflight.Group{},
	}
}

func (hc _HandlerSingleflight) Exec(sql string) (err error) {
	_, err, _ = hc.group.Do(sql, func() (interface{}, error) {
		return nil, hc.handler.Exec(sql)
	})
	return err
}
func (hc _HandlerSingleflight) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
	dbExecResultAny, err, _ := hc.group.Do(sql, func() (interface{}, error) {
		rowsAffected, err := hc.handler.ExecWithRowsAffected(sql)
		if err != nil {
			return 0, err
		}
		result := _DbExecResult{
			rowsAffected: rowsAffected,
			lastInsertId: 0,
		}
		return result, nil
	})
	if err != nil {
		return 0, err
	}
	dbExecResult := dbExecResultAny.(_DbExecResult)
	rowsAffected = dbExecResult.rowsAffected
	return rowsAffected, nil
}
func (hc _HandlerSingleflight) InsertWithLastIdHandler(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
	dbExecResultAny, err, _ := hc.group.Do(sql, func() (interface{}, error) {
		lastInsertId, rowsAffected, err := hc.handler.InsertWithLastIdHandler(sql)
		if err != nil {
			return nil, err
		}
		result := _DbExecResult{
			rowsAffected: rowsAffected,
			lastInsertId: lastInsertId,
		}
		return result, nil
	})
	if err != nil {
		return 0, 0, err
	}
	dbExecResult := dbExecResultAny.(_DbExecResult)
	lastInsertId = dbExecResult.lastInsertId
	rowsAffected = dbExecResult.rowsAffected
	return lastInsertId, rowsAffected, nil
}
func (hc _HandlerSingleflight) First(sql string, result any) (exists bool, err error) {
	rv := reflect.Indirect(reflect.ValueOf(result))
	dbExecResultAny, err, _ := hc.group.Do(sql, func() (interface{}, error) {
		v := reflect.New(rv.Type()).Interface()
		exists, err := hc.handler.First(sql, v)
		if err != nil {
			return nil, err
		}
		result := _DbExecResult{
			data:   result,
			exists: exists,
		}
		return result, nil
	})
	if err != nil {
		return false, err
	}
	dbExecResult := dbExecResultAny.(_DbExecResult)
	SetReflectValue(rv, reflect.ValueOf(dbExecResult.data))
	exists = dbExecResult.exists
	return exists, nil
}
func (hc _HandlerSingleflight) Query(sql string, result any) (err error) {
	rv := reflect.Indirect(reflect.ValueOf(result))
	dbExecResultAny, err, _ := hc.group.Do(sql, func() (interface{}, error) {
		v := reflect.New(rv.Type()).Interface()
		err := hc.handler.Query(sql, v)
		if err != nil {
			return nil, err
		}
		result := _DbExecResult{
			data: result,
		}
		return result, nil
	})
	if err != nil {
		return err
	}
	dbExecResult := dbExecResultAny.(_DbExecResult)
	SetReflectValue(rv, reflect.ValueOf(dbExecResult.data))
	return nil
}
func (hc _HandlerSingleflight) Count(sql string) (count int64, err error) {
	countAny, err, _ := hc.group.Do(sql, func() (interface{}, error) {
		count, err := hc.handler.Count(sql)
		if err != nil {
			return 0, err
		}
		return count, nil
	})
	if err != nil {
		return 0, err
	}
	count = countAny.(int64)
	return count, nil

}
func (hc _HandlerSingleflight) Exists(sql string) (exists bool, err error) {
	existsAny, err, _ := hc.group.Do(sql, func() (interface{}, error) {
		exists, err := hc.handler.Exists(sql)
		if err != nil {
			return 0, err
		}
		return exists, nil
	})
	if err != nil {
		return false, err
	}
	exists = existsAny.(bool)
	return exists, nil

}

type _HandlerCache struct {
	handler Handler
}

func WithCache(handler Handler) Handler {
	return &_HandlerCache{
		handler: handler,
	}
}

func (hc _HandlerCache) Exec(sql string) (err error) {
	return hc.handler.Exec(sql)
}
func (hc _HandlerCache) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
	return hc.handler.ExecWithRowsAffected(sql)
}
func (hc _HandlerCache) InsertWithLastIdHandler(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
	return hc.handler.InsertWithLastIdHandler(sql)
}
func (hc _HandlerCache) First(sql string, result any) (exists bool, err error) {
	cacheResult := _DbExecResult{}
	rv := reflect.Indirect(reflect.ValueOf(result))
	err = cache.Remember(sql, 1*time.Minute, &cacheResult, func() (any, error) {
		result := reflect.New(rv.Type()).Interface()
		exists, err := hc.handler.First(sql, result)
		if err != nil {
			return nil, err
		}
		cacheResult := _DbExecResult{
			data:   result,
			exists: exists,
		}
		return cacheResult, nil
	})
	if err != nil {
		return false, err
	}
	SetReflectValue(rv, reflect.ValueOf(cacheResult.data))
	exists = cacheResult.exists
	return exists, nil
}
func (hc _HandlerCache) Query(sql string, result any) (err error) {
	rv := reflect.Indirect(reflect.ValueOf(result))
	err = cache.Remember(sql, 1*time.Minute, result, func() (any, error) {
		data := reflect.New(rv.Type()).Interface()
		err := hc.handler.Query(sql, data)
		if err != nil {
			return nil, err
		}
		return data, nil
	})
	if err != nil {
		return err
	}
	return nil
}
func (hc _HandlerCache) Count(sql string) (count int64, err error) {
	err = cache.Remember(sql, 1*time.Minute, &count, func() (any, error) {
		count, err := hc.handler.Count(sql)
		if err != nil {
			return 0, err
		}
		return count, nil
	})
	if err != nil {
		return 0, err
	}
	return count,
		nil
}
func (hc _HandlerCache) Exists(sql string) (exists bool, err error) {
	err = cache.Remember(sql, 1*time.Minute, &exists, func() (any, error) {
		exists, err := hc.handler.Exists(sql)
		if err != nil {
			return false, err
		}
		return exists, nil
	})
	if err != nil {
		return false, err
	}
	return exists, nil
}

func SetReflectValue(dst reflect.Value, src reflect.Value) {
	rdst := reflect.Indirect(dst)
	rsrc := reflect.Indirect(src)
	if rsrc.CanConvert(rdst.Type()) {
		rsrc = rsrc.Convert(rdst.Type())
	}
	rdst.Set(rsrc)
}
