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

// func WarpUpdateWithEventTrigger(updateHander ExecWithRowsAffectedHandler, eventUpdateTrigger EventUpdateTrigger) ExecWithRowsAffectedHandler {
// 	return func(sql string) (rowsAffected int64, err error) {
// 		rowsAffected, err = updateHander(sql)
// 		if err != nil {
// 			return
// 		}

// 		if eventUpdateTrigger != nil {
// 			err = eventUpdateTrigger(rowsAffected)
// 			if err != nil {
// 				return rowsAffected, err
// 			}
// 		}
// 		return rowsAffected, nil
// 	}
// }

// func WarpInsertWithEventTrigger(insertHander InsertWithLastIdHandler, eventInsertTrigger EventInsertTrigger) InsertWithLastIdHandler {
// 	return func(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
// 		lastInsertId, rowsAffected, err = insertHander(sql)
// 		if err != nil {
// 			return
// 		}

// 		if eventInsertTrigger != nil {
// 			err = eventInsertTrigger(lastInsertId, rowsAffected)
// 			if err != nil {
// 				return lastInsertId, rowsAffected, err
// 			}
// 		}
// 		return lastInsertId, rowsAffected, nil
// 	}
// }

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
}

func NewCompiler(cfg CompilerConfig, fs ...*Field) Compiler {
	/* 	if len(fs) == 0 {
		panic(errors.New("sqlbuilder: Compiler need fields"))// 有的场景确实不需要字段，比如列表查询时，无筛选条件
	} */
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
	//return NewInsertBuilder(h.Table()).WithHandler(h.Handler().InsertWithLastIdHandler).WithTriggerEvent(h.insertEvent).AppendFields(h.Fields()...)
	return NewInsertBuilder(h.Table()).WithHandler(h.Handler()).AppendFields(h.Fields()...)
}
func (h Compiler) InsertBatch() *BatchInsertParam {
	//return NewBatchInsertBuilder(h.Table()).WithHandler(h.Handler().InsertWithLastIdHandler).WithTriggerEvent(h.insertEvent).AppendFields(h.BatchFields()...)
	return NewBatchInsertBuilder(h.Table()).WithHandler(h.Handler()).AppendFields(h.BatchFields()...)
}

func (h Compiler) Update() *UpdateParam {
	//return NewUpdateBuilder(h.Table()).WithHandler(h.Handler().ExecWithRowsAffected).WithTriggerEvent(h.updateEvent).AppendFields(h.Fields()...)
	return NewUpdateBuilder(h.Table()).WithHandler(h.Handler()).AppendFields(h.Fields()...)
}

func (h Compiler) Delete() *DeleteParam {
	//return NewDeleteBuilder(h.Table()).WithHandler(h.Handler().ExecWithRowsAffected).WithTriggerEvent(h.deleteEvent).AppendFields(h.Fields()...)
	return NewDeleteBuilder(h.Table()).WithHandler(h.Handler()).AppendFields(h.Fields()...)
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
	return NewSetBuilder(h.Table()).WithHandler(h.Handler()).AppendFields(h.Fields()...)
}

type Handler interface {
	Exec(sql string) (err error)
	ExecWithRowsAffected(sql string) (rowsAffected int64, err error)
	InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error)
	First(sql string, result any) (exists bool, err error)
	Query(sql string, result any) (err error)
	Count(sql string) (count int64, err error)
	Exists(sql string) (exists bool, err error)
	OriginalHandler() Handler // 获取原始handler，有时候需要绕过各种中间件，获取原始handler，比如 set 操作判断是否存在时，要绕过缓存中间件
	IsOriginalHandler() bool  // 是否是原始handler，比如gorm的handler就是原始handler, 其它 增加缓存、事件、单实例等都不是原始handler. 增加该接口后，非原始handler，可以多次嵌套，这在事件中间件中非常有用，使得事件handler 可以设置统一处理器，也可以单独设置(需要时再包裹一次就行)

}

type GormHandler func() *gorm.DB

func NewGormHandler(getDB func() *gorm.DB) Handler {
	return GormHandler(getDB)
}

func (h GormHandler) OriginalHandler() Handler {
	return h
}
func (h GormHandler) IsOriginalHandler() bool {
	return true
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
func (h GormHandler) InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
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

func GetOriginalHandler(h Handler) Handler {
	originalHandler := h
	maxLoop := 1000 // 防止无限循环，理论上不应该出现这种情况，如果出现，说明代码有问题

	for {
		maxLoop--
		if originalHandler.IsOriginalHandler() {
			return originalHandler
		}
		originalHandler = originalHandler.OriginalHandler()
		if maxLoop <= 0 {
			err := errors.New("too many loop for GetOriginalHandler")
			panic(err)
		}
	}
}

type _HandlerSingleflight struct {
	handler Handler
	group   *singleflight.Group
}

func WithSingleflight(handler Handler) Handler {
	return _HandlerSingleflight{
		handler: handler,
		group:   &singleflight.Group{},
	}
}

func (hc _HandlerSingleflight) OriginalHandler() Handler {
	return GetOriginalHandler(hc.handler)
}
func (hc _HandlerSingleflight) IsOriginalHandler() bool {
	return false
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
func (hc _HandlerSingleflight) InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
	dbExecResultAny, err, _ := hc.group.Do(sql, func() (interface{}, error) {
		lastInsertId, rowsAffected, err := hc.handler.InsertWithLastId(sql)
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
			data:   v,
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
			data: v,
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
	return _HandlerCache{
		handler: handler,
	}
}

var Cache_sql_duration time.Duration = 1 * time.Minute

func (hc _HandlerCache) OriginalHandler() Handler {
	return GetOriginalHandler(hc.handler)
}

func (hc _HandlerCache) IsOriginalHandler() bool {
	return false
}

func (hc _HandlerCache) Exec(sql string) (err error) {
	return hc.handler.Exec(sql)
}
func (hc _HandlerCache) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
	return hc.handler.ExecWithRowsAffected(sql)
}
func (hc _HandlerCache) InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
	return hc.handler.InsertWithLastId(sql)
}
func (hc _HandlerCache) First(sql string, result any) (exists bool, err error) {
	cacheResult := _DbExecResult{}
	rv := reflect.Indirect(reflect.ValueOf(result))
	err = cache.Remember(sql, Cache_sql_duration, &cacheResult, func() (any, error) {
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
	err = cache.Remember(sql, Cache_sql_duration, result, func() (any, error) {
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
	err = cache.Remember(sql, Cache_sql_duration, &count, func() (any, error) {
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
	err = cache.Remember(sql, Cache_sql_duration, &exists, func() (any, error) {
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

//_HandlerSingleflightDoOnce 单例执行一次，防止并发问题,目前用于Set 中的exists 查询，所以只实现 exists 查询

type _HandlerSingleflightDoOnce struct {
	handler Handler
	group   *singleflight.Group
}

func WithSingleflightDoOnce(handler Handler) Handler {
	return _HandlerSingleflightDoOnce{
		handler: handler,
		group:   &singleflight.Group{},
	}
}

func (hc _HandlerSingleflightDoOnce) OriginalHandler() Handler {
	return GetOriginalHandler(hc.handler)
}
func (hc _HandlerSingleflightDoOnce) IsOriginalHandler() bool {
	return false
}

func (hc _HandlerSingleflightDoOnce) Exec(sql string) (err error) {
	return hc.handler.Exec(sql)
}
func (hc _HandlerSingleflightDoOnce) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
	return hc.handler.ExecWithRowsAffected(sql)
}
func (hc _HandlerSingleflightDoOnce) InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
	return hc.handler.InsertWithLastId(sql)
}
func (hc _HandlerSingleflightDoOnce) First(sql string, result any) (exists bool, err error) {
	return hc.handler.First(sql, result)
}
func (hc _HandlerSingleflightDoOnce) Query(sql string, result any) (err error) {
	return hc.handler.Query(sql, result)
}
func (hc _HandlerSingleflightDoOnce) Count(sql string) (count int64, err error) {
	return hc.handler.Count(sql)

}
func (hc _HandlerSingleflightDoOnce) Exists(sql string) (exists bool, err error) {
	existsAny, err, shared := hc.group.Do(sql, func() (interface{}, error) {
		exists, err := hc.handler.Exists(sql)
		if err != nil {
			return 0, err
		}
		return exists, nil
	})
	if err != nil {
		return false, err
	}
	if shared {
		err = errors.Errorf("the value has already been shared once")
		return false, err
	}
	exists = existsAny.(bool)
	return exists, nil
}

const (
	Event_Operation_Insert = "insert"
	Event_Operation_Update = "update"
	Event_Operation_Delete = "delete"
)

type Direction string

type Event struct {
	Operation    string // 操作类型
	LastInsertId uint64
	RowsAffected int64
	SQL          string
}

type EventASyncHandler func(event *Event)

// _HandlerTriggerAsyncEvent 只触发数据库层面的事件,主要用于数据冗余同步更新,区分不了数据库层面更新、业务层面为delete场景，如果需要触发业务层面的事件,请监听更上层事件
type _HandlerTriggerAsyncEvent struct {
	handler               Handler
	_eventAsyncDispatcher EventASyncHandler
}

func WithTriggerAsyncEvent(handler Handler, eventHandler EventASyncHandler) Handler {
	return _HandlerTriggerAsyncEvent{
		handler:               handler,
		_eventAsyncDispatcher: eventHandler,
	}
}

// getEventLeaveDispatcher 执行SQL后触发事件,不关心结果
func (hc _HandlerTriggerAsyncEvent) getEventLeaveDispatcher() EventASyncHandler {
	if hc._eventAsyncDispatcher != nil {
		return hc._eventAsyncDispatcher
	}
	return func(event *Event) {}
}

func (hc _HandlerTriggerAsyncEvent) OriginalHandler() Handler {
	return GetOriginalHandler(hc.handler)
}
func (hc _HandlerTriggerAsyncEvent) IsOriginalHandler() bool {
	return false
}
func (hc _HandlerTriggerAsyncEvent) Exec(sql string) (err error) {
	err = hc.handler.Exec(sql)
	if err != nil {
		return err
	}
	event := &Event{
		Operation: Event_Operation_Update,
		SQL:       sql,
	}
	go hc.getEventLeaveDispatcher()(event)
	return nil
}

func (hc _HandlerTriggerAsyncEvent) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
	rowsAffected, err = hc.handler.ExecWithRowsAffected(sql)
	if err != nil {
		return
	}
	event := &Event{
		Operation:    Event_Operation_Update,
		RowsAffected: rowsAffected,
		SQL:          sql,
	}
	go hc.getEventLeaveDispatcher()(event)

	return rowsAffected, nil
}

func (hc _HandlerTriggerAsyncEvent) InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error) {
	lastInsertId, rowsAffected, err = hc.handler.InsertWithLastId(sql)
	if err != nil {
		return 0, 0, err
	}
	event := &Event{
		Operation:    Event_Operation_Insert,
		LastInsertId: lastInsertId,
		RowsAffected: rowsAffected,
		SQL:          sql,
	}
	go hc.getEventLeaveDispatcher()(event)

	return lastInsertId, rowsAffected, nil
}
func (hc _HandlerTriggerAsyncEvent) First(sql string, result any) (exists bool, err error) {
	return hc.handler.First(sql, result)
}
func (hc _HandlerTriggerAsyncEvent) Query(sql string, result any) (err error) {
	return hc.handler.Query(sql, result)
}
func (hc _HandlerTriggerAsyncEvent) Count(sql string) (count int64, err error) {
	return hc.handler.Count(sql)

}
func (hc _HandlerTriggerAsyncEvent) Exists(sql string) (exists bool, err error) {
	return hc.handler.Exists(sql)
}
