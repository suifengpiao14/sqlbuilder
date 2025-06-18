package sqlbuilder

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	_ "gorm.io/driver/mysql"
	_ "gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type BuilderFn func() Builder

type Builder struct {
	handler Handler
	table   TableConfig
}

func NewGormBuilder(table TableConfig, getDB func() *gorm.DB) Builder {
	handler := NewGormHandler(getDB)
	return NewBuilder(table, handler)
}

func NewBuilder(table TableConfig, handler Handler) Builder { // 因为 WithHandler 需要复制，所以这里统一不返回地址值
	return Builder{handler: handler, table: table}
}

func (b Builder) WithHandler(handler Handler) Builder { // transaction 时候需要重新设置handler
	return Builder{table: b.table, handler: handler}
}

func (b Builder) Handler() (handler Handler) { // 提供给外部使用
	return b.handler
}

func (b Builder) TotalParam(fs ...*Field) *TotalParam {
	p := NewTotalBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p

}
func (b Builder) ListParam(fs ...*Field) *ListParam {
	p := NewListBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p

}

func (b Builder) PaginationParam(fs ...*Field) *PaginationParam {
	p := NewPaginationBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p
}
func (b Builder) FirstParam(fs ...*Field) *FirstParam {
	p := NewFirstBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p
}
func (b Builder) InsertParam(fs ...*Field) *InsertParam {
	p := NewInsertBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p
}
func (b Builder) BatchInsertParam(fss ...Fields) *BatchInsertParam {
	p := NewBatchInsertBuilder(b.table).WithHandler(b.handler).AppendFields(fss...)
	return p

}
func (b Builder) UpdateParam(fs ...*Field) *UpdateParam {
	p := NewUpdateBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p
}
func (b Builder) DeleteParam(fs ...*Field) *DeleteParam {
	p := NewDeleteBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p
}

func (b Builder) ExistsParam(fs ...*Field) *ExistsParam {
	p := NewExistsBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p
}
func (b Builder) SetParam(fs ...*Field) *SetParam {
	p := NewSetBuilder(b.table).WithHandler(b.handler).AppendFields(fs...)
	return p
}

func (b Builder) Count(fields ...*Field) (count int64, err error) {
	return b.TotalParam().AppendFields(fields...).Count()
}

func (b Builder) List(result any, fields ...*Field) (err error) {
	return b.ListParam().AppendFields(fields...).Query(result)
}

func (b Builder) Pagination(result any, fields ...*Field) (count int64, err error) {
	return b.PaginationParam().AppendFields(fields...).Pagination(result)
}

func (b Builder) First(result any, fields ...*Field) (exists bool, err error) {
	return b.FirstParam().AppendFields(fields...).First(result)
}

func (b Builder) Insert(fields ...*Field) (err error) {
	return b.InsertParam().AppendFields(fields...).Exec()
}
func (b Builder) BatchInsert(fields ...Fields) (err error) {
	return b.BatchInsertParam().AppendFields(fields...).Exec()
}

func (b Builder) Update(fields ...*Field) (err error) {
	return b.UpdateParam().AppendFields(fields...).Exec()
}

func (b Builder) Delete(fields ...*Field) (err error) {
	return b.DeleteParam().AppendFields(fields...).Exec()
}

func (b Builder) Exists(fields ...*Field) (exists bool, err error) {
	return b.ExistsParam().AppendFields(fields...).Exists()
}
func (b Builder) Set(fields ...*Field) (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	return b.SetParam().AppendFields(fields...).Set()
}

type Driver string

func (d Driver) String() string {
	return string(d)
}

func (d Driver) IsSame(target Driver) bool {
	return strings.EqualFold(d.String(), target.String())
}

type Expressions = []goqu.Expression

var ERROR_EMPTY_WHERE = errors.New("error  empty where")

const (
	Driver_mysql   Driver = "mysql"
	Driver_sqlite3 Driver = "sqlite3"
	_Driver_sqlite Driver = "sqlite" // 内部使用
)

var MysqlEscapeString = func(val string) string {
	dest := make([]byte, 0, 2*len(val))
	var escape byte
	for i := 0; i < len(val); i++ {
		c := val[i]
		escape = 0
		switch c {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
		case '\n': /* Must be escaped for logs */
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"': /* Better safe than sorry */
			escape = '"'
		case '\032': //十进制26,八进制32,十六进制1a, /* This gives problems on Win32 */
			escape = 'Z'
		}

		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}

	return string(dest)

}
var SQLite3EscapeString = MysqlEscapeString // 此处暂时使用mysql的

type DialectWrapper struct {
	dialect string
}

func (d DialectWrapper) Dialect() string {
	return d.dialect
}
func (d DialectWrapper) DialectWrapper() goqu.DialectWrapper {
	return goqu.Dialect(d.dialect)
}

func (d DialectWrapper) EscapeString(val string) string {
	switch strings.ToLower(d.dialect) {
	case strings.ToLower(Driver_mysql.String()):
		return MysqlEscapeString(val)
	case strings.ToLower(Driver_sqlite3.String()):
		return SQLite3EscapeString(val)
	}
	return val
}

func NewDialect(dialect string) DialectWrapper {
	return DialectWrapper{
		dialect: dialect,
	}
}

// Dialect 设定驱动,方便直接使用
var Dialect = NewDialect(Driver_sqlite3.String())

var Dialect_Mysql = NewDialect(Driver_mysql.String())

func EscapeString(s string) (escaped string) {
	escaped = Dialect.EscapeString(s)
	return escaped
}

type Scene string // 迁移场景

type Scenes []Scene

func (s Scene) Is(target Scene) bool {
	return strings.EqualFold(string(s), string(target))
}

const (
	SCENE_SQL_INIT   Scene = "init" // 场景初始化，常用于清除前期设置，如当前字段只用于搜索(入参用于在2个字段上搜索)，其它场景不存在
	SCENE_SQL_INSERT Scene = "insert"
	SCENE_SQL_UPDATE Scene = "update"
	SCENE_SQL_DELETE Scene = "delete"
	SCENE_SQL_SELECT Scene = "select"
	//SCENE_SQL_EXISTS   Scene = "exists"
	SCENE_SQL_VIEW     Scene = "view"
	SCENE_SQL_INCREASE Scene = "increse" // 字段递增
	SCENE_SQL_DECREASE Scene = "decrese" // 字段递减
	SCENE_SQL_FINAL    Scene = "final"   // 最终状态(所有场景执行完成后再执行的场景 ,有时需要清除公共库设置的所有场景，只有在这里清除最保险)
)

// 操作数据库场景
var SCENE_Commands = Scenes{SCENE_SQL_INSERT, SCENE_SQL_UPDATE, SCENE_SQL_DELETE, SCENE_SQL_INCREASE, SCENE_SQL_DECREASE}

type TableI interface {
	TableConfig() (table TableConfig)
}
type TableFn func() (table TableConfig)

func (fn TableFn) TableConfig() (table TableConfig) {
	return fn()
}

// type _Select interface {
// 	Select() (columns []any)
// }

func ConcatOrderedExpression(orderedExpressions ...exp.OrderedExpression) []exp.OrderedExpression {
	return orderedExpressions
}

func ConcatExpression(expressions ...exp.Expression) Expressions {
	return expressions
}

// 设置字段支持多阶段设置，(比如 最初的调用入口、封装的服务固定字段、以及表级别的字段)CustomFieldsFn 用于在所有字段合并后给用户一个入口修改最终字段列表的机会(可扩展性更强)
type CustomFieldsFn func(fs Fields) (customedFs Fields)

type CustomFieldsFns []CustomFieldsFn

// InsertParam 供子类复用,修改数据
type InsertParam struct {
	_Table  TableI
	_Fields Fields
	_log    LogI
	//execHandler             ExecHandler
	handler             Handler // 支持事务句柄
	_triggerInsertEvent EventInsertTrigger
	context             context.Context
	customFieldsFns     CustomFieldsFns
}

func (p *InsertParam) WithContext(ctx context.Context) *InsertParam {
	p.context = ctx
	return p
}

func (p *InsertParam) WithCustomFieldsFn(customFields ...CustomFieldsFn) *InsertParam {
	p.customFieldsFns = customFields
	return p
}

func (p *InsertParam) WithTriggerEvent(triggerInsertEvent EventInsertTrigger) *InsertParam {
	p._triggerInsertEvent = triggerInsertEvent
	return p
}
func (p *InsertParam) getEventHandler() (triggerInsertEvent EventInsertTrigger) {
	triggerInsertEvent = p._triggerInsertEvent
	if triggerInsertEvent == nil {
		triggerInsertEvent = func(lastInsertId uint64, rowsAffected int64) (err error) { return }
	}
	return triggerInsertEvent
}

func (p *InsertParam) SetLog(log LogI) InsertParam {
	p._log = log
	return *p
}

func (p *InsertParam) WithHandler(handler Handler) *InsertParam {
	//p.execHandler = execHandler
	p.handler = handler
	return p
}

func NewInsertBuilder(tableConfig TableConfig) *InsertParam {
	return &InsertParam{
		_Table:  TableFn(func() TableConfig { return tableConfig }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p *InsertParam) AppendFields(fields ...*Field) *InsertParam {
	p._Fields.Append(fields...)
	return p
}
func (p InsertParam) GetTable() (table TableConfig) {
	return p._Table.TableConfig()
}

func (p *InsertParam) ApplyCustomFn(customFn CustomFnInsertParam) *InsertParam {
	if customFn != nil {
		customFn(p)
	}
	return p
}

func (p InsertParam) ToSQL() (sql string, err error) {
	if p._Table == nil {
		err = errors.Errorf("InsertParam._Table required")
		return "", err
	}
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_INSERT, tableConfig, p.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量

	// err = tableConfig.CheckUniqueIndex(fs...) // 不能内置，直接检测，应为 setParam 需要生成insert 语句，这段代码直接执行，会导致生成insert语句时报错
	// if err != nil {
	// 	return "", err
	// }

	rowData, err := fs.Data(layer_order...)
	if err != nil {
		return "", err
	}
	if IsNil(rowData) {
		err = errors.New("InsertParam.Data() return nil data")
		return "", err
	}

	ds := Dialect.DialectWrapper().Insert(tableConfig.Name).Rows(rowData)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

func (p InsertParam) Validate() (err error) {
	fs := p._Fields.Copy()
	fs.SetSceneIfEmpty(SCENE_SQL_INSERT)
	err = fs.Validate()
	if err != nil {
		return err
	}
	return nil
}

func (p InsertParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.LastInsertId, event.RowsAffected)
	})
	_, _, err = withEventHandler.InsertWithLastId(sql)
	return err
}

// Deprecated: use Insert instead
func (p InsertParam) InsertWithLastId() (lastInsertId uint64, rowsAffected int64, err error) {
	return p.Insert()
}

func (p InsertParam) Insert() (lastInsertId uint64, rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return 0, 0, err
	}
	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.LastInsertId, event.RowsAffected)
	})
	return withEventHandler.InsertWithLastId(sql)
}

type BatchInsertParam struct {
	rowFields []Fields
	_Table    TableI
	_log      LogI
	//execHandler             ExecHandler
	handler             Handler // 支持事务句柄
	_triggerInsertEvent EventInsertTrigger
	context             context.Context
	customFieldsFns     CustomFieldsFns
}

func (p *BatchInsertParam) WithContext(ctx context.Context) *BatchInsertParam {
	p.context = ctx
	return p
}

func (p *BatchInsertParam) WithCustomFieldsFn(customFieldsFns ...CustomFieldsFn) *BatchInsertParam {
	p.customFieldsFns = customFieldsFns
	return p
}

func (p *BatchInsertParam) WithTriggerEvent(triggerInsertEvent EventInsertTrigger) *BatchInsertParam {
	p._triggerInsertEvent = triggerInsertEvent
	return p
}
func (p *BatchInsertParam) getEventHandler() (triggerInsertEvent EventInsertTrigger) {
	triggerInsertEvent = p._triggerInsertEvent
	if triggerInsertEvent == nil {
		triggerInsertEvent = func(lastInsertId uint64, rowsAffected int64) (err error) { return }
	}
	return triggerInsertEvent
}

func NewBatchInsertBuilder(tableConfig TableConfig) *BatchInsertParam {
	return &BatchInsertParam{
		_Table:    TableFn(func() TableConfig { return tableConfig }),
		rowFields: make([]Fields, 0),
		_log:      DefaultLog,
	}
}

func (p *BatchInsertParam) SetLog(log LogI) *BatchInsertParam {
	p._log = log
	return p
}

func (p *BatchInsertParam) WithHandler(handler Handler) *BatchInsertParam {
	p.handler = handler
	return p
}

func (p *BatchInsertParam) AppendFields(fields ...Fields) *BatchInsertParam {
	if p.rowFields == nil {
		p.rowFields = make([]Fields, 0)
	}
	p.rowFields = append(p.rowFields, fields...)
	return p
}

var ERROR_BATCH_INSERT_DATA_IS_NIL = errors.New("batch insert err: data is nil")
var ERROR_NOT_FOUND = errors.New("not found record")

func (is BatchInsertParam) GetTable() TableConfig {
	return is._Table.TableConfig()
}
func (p *BatchInsertParam) ApplyCustomFn(customFn CustomFnBatchInsertParam) *BatchInsertParam {
	if customFn != nil {
		customFn(p)
	}
	return p
}

func (is BatchInsertParam) ToSQL() (sql string, err error) {
	data := make([]any, 0)

	tableConfig := is.GetTable()
	for _, fields := range is.rowFields {
		fs := fields.Builder(is.context, SCENE_SQL_INSERT, tableConfig, is.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
		rowData, err := fs.Data(layer_order...)
		if err != nil {
			return "", err
		}
		if IsNil(rowData) {
			continue
		}
		data = append(data, rowData)
	}
	if len(data) == 0 {
		return "", ERROR_BATCH_INSERT_DATA_IS_NIL
	}
	ds := Dialect.DialectWrapper().Insert(tableConfig.Name).Rows(data...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if is._log != nil {
		is._log.Log(sql)
	}
	return sql, nil
}
func (p BatchInsertParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}

	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.LastInsertId, event.RowsAffected)
	})
	_, _, err = withEventHandler.InsertWithLastId(sql)
	return err
}
func (p BatchInsertParam) InsertWithLastId() (lastInsertId uint64, rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return 0, 0, err
	}
	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.LastInsertId, event.RowsAffected)
	})
	return withEventHandler.InsertWithLastId(sql)
}

type DeleteParam struct {
	_Table               TableI
	_Fields              Fields
	_log                 LogI
	handler              Handler // 支持事务句柄
	_triggerDeletedEvent EventDeletedTrigger
	context              context.Context
	customFieldsFns      CustomFieldsFns
}

func (p *DeleteParam) WithContext(ctx context.Context) DeleteParam {
	p.context = ctx
	return *p
}

func (p *DeleteParam) WithCustomFieldsFn(customFieldsFns ...CustomFieldsFn) *DeleteParam {
	p.customFieldsFns = customFieldsFns
	return p
}

func (p *DeleteParam) WithTriggerEvent(triggerDeletedEvent EventDeletedTrigger) *DeleteParam {
	p._triggerDeletedEvent = triggerDeletedEvent
	return p
}
func (p *DeleteParam) getEventHandler() (triggerDeletedEvent EventDeletedTrigger) {
	triggerDeletedEvent = p._triggerDeletedEvent
	if triggerDeletedEvent == nil {
		triggerDeletedEvent = func(rowsAffected int64) (err error) { return }
	}
	return triggerDeletedEvent
}

func (p *DeleteParam) SetLog(log LogI) DeleteParam {
	p._log = log
	return *p
}
func (p *DeleteParam) WithHandler(handler Handler) *DeleteParam {
	p.handler = handler
	return p
}

func NewDeleteBuilder(tableConfig TableConfig) *DeleteParam {
	return &DeleteParam{
		_Table:  TableFn(func() TableConfig { return tableConfig }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p *DeleteParam) AppendFields(fields ...*Field) *DeleteParam {
	p._Fields.Append(fields...)
	return p
}

func (p DeleteParam) GetTable() TableConfig {
	return p._Table.TableConfig()
}
func (p *DeleteParam) ApplyCustomFn(customFn CustomFnDeleteParam) *DeleteParam {
	if customFn != nil {
		customFn(p)
	}
	return p
}

func (p DeleteParam) ToSQL() (sql string, err error) {
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_DELETE, tableConfig, p.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	f, err := fs.DeletedAt()
	if err != nil {
		return "", err
	}
	canUpdateFields := fs.GetByTags(Field_tag_CanWriteWhenDeleted)
	canUpdateFields.Append(f)
	data, err := canUpdateFields.Data(layer_order...)
	if err != nil {
		return "", err
	}

	where, err := fs.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.DialectWrapper().Update(tableConfig.Name).Set(data).Where(where...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}
func (p DeleteParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.RowsAffected)
	})
	_, err = withEventHandler.ExecWithRowsAffected(sql)
	return err
}
func (p DeleteParam) Delete() (rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return rowsAffected, err
	}
	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.RowsAffected)
	})
	rowsAffected, err = withEventHandler.ExecWithRowsAffected(sql)
	return rowsAffected, err
}

// deprecated use Delete instead
func (p DeleteParam) ExecWithRowsAffected() (rowsAffected int64, err error) {
	return p.Delete()
}

type UpdateParam struct {
	_Table               TableI
	_Fields              Fields
	_log                 LogI
	handler              Handler // 支持事务句柄
	_triggerUpdatedEvent EventUpdateTrigger
	context              context.Context
	customFieldsFns      CustomFieldsFns
}

func (p *UpdateParam) WithContext(ctx context.Context) *UpdateParam {
	p.context = ctx
	return p
}

func (p *UpdateParam) WithCustomFieldsFn(customFieldsFns ...CustomFieldsFn) *UpdateParam {
	p.customFieldsFns = customFieldsFns
	return p
}

func (p *UpdateParam) WithTriggerEvent(triggerUpdateEvent EventUpdateTrigger) *UpdateParam {
	p._triggerUpdatedEvent = triggerUpdateEvent
	return p
}
func (p *UpdateParam) getEventHandler() (triggerUpdateEvent EventUpdateTrigger) {
	triggerUpdateEvent = p._triggerUpdatedEvent
	if triggerUpdateEvent == nil {
		triggerUpdateEvent = func(rowsAffected int64) (err error) { return nil }
	}
	return triggerUpdateEvent
}

func (p *UpdateParam) SetLog(log LogI) UpdateParam {
	p._log = log
	return *p
}
func (p *UpdateParam) WithHandler(handler Handler) *UpdateParam {
	p.handler = handler
	return p
}

func NewUpdateBuilder(tableConfig TableConfig) *UpdateParam {
	return &UpdateParam{
		_Table:  TableFn(func() TableConfig { return tableConfig }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p *UpdateParam) AppendFields(fields ...*Field) *UpdateParam {
	p._Fields.Append(fields...)
	return p
}

func (p UpdateParam) GetTable() TableConfig {
	return p._Table.TableConfig()
}

func (p *UpdateParam) ApplyCustomFn(customFn CustomFnUpdateParam) *UpdateParam {
	if customFn != nil {
		customFn(p)
	}
	return p
}

func (p UpdateParam) ToSQL() (sql string, err error) {
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_UPDATE, tableConfig, p.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	data, err := fs.Data(layer_order...)
	if err != nil {
		return "", err
	}

	where, err := fs.Where()
	if err != nil {
		return "", err
	}
	if len(where) == 0 {
		err = errors.New("update must have where condition")
		return "", err
	}
	limit := fs.Limit()

	ds := Dialect.DialectWrapper().Update(tableConfig.Name).Set(data).Where(where...).Order(fs.Order()...)
	if limit > 0 {
		ds = ds.Limit(limit)
	}

	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

func (p UpdateParam) Validate() (err error) {
	fs := p._Fields.Copy()
	fs.SetSceneIfEmpty(SCENE_SQL_UPDATE)
	err = fs.Validate()
	if err != nil {
		return err
	}
	return nil
}
func (p UpdateParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.RowsAffected)
	})
	_, err = withEventHandler.ExecWithRowsAffected(sql)
	return err
}

// Deprecated :已废弃,请使用Update
func (p UpdateParam) ExecWithRowsAffected() (rowsAffected int64, err error) {
	return p.Update()
}

func (p UpdateParam) Update() (rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return 0, err
	}
	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.RowsAffected)
	})
	rowsAffected, err = withEventHandler.ExecWithRowsAffected(sql)
	return rowsAffected, err
}

func (p UpdateParam) UpdateMustExists() (rowsAffected int64, err error) {

	cp := p._Fields.Copy()
	existsParam := NewExistsBuilder(p.GetTable()).AppendFields(cp...)
	exists, err := existsParam.Exists()
	if err != nil {
		return 0, err
	}
	if !exists {
		existsSql, _ := existsParam.ToSQL()
		err = errors.Errorf("record not exists with the sql:%s", existsSql)
		return 0, err
	}

	sql, err := p.ToSQL()
	if err != nil {
		return 0, err
	}
	withEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		p.getEventHandler()(event.RowsAffected)
	})
	rowsAffected, err = withEventHandler.ExecWithRowsAffected(sql)
	return rowsAffected, err
}

type Context_Key string

const (
	Context_key_CacheDuration Context_Key = "Context_CacheDuration"
)

func WithCacheDuration(ctx context.Context, duration time.Duration) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if duration <= 0 {
		return ctx
	}
	return context.WithValue(ctx, Context_key_CacheDuration, duration)
}
func GetCacheDuration(ctx context.Context) time.Duration {
	if ctx == nil {
		return 0
	}
	if v, ok := ctx.Value(Context_key_CacheDuration).(time.Duration); ok {
		return v
	}
	return 0
}

type FirstParam struct {
	_Table          TableI
	_Fields         Fields
	_log            LogI
	handler         Handler // 支持事务句柄
	builderFns      SelectBuilderFns
	context         context.Context
	customFieldsFns CustomFieldsFns
}

func (p *FirstParam) WithContext(ctx context.Context) *FirstParam {
	p.context = ctx
	return p
}

func (p *FirstParam) WithCustomFieldsFn(customFieldsFns ...CustomFieldsFn) *FirstParam {
	p.customFieldsFns = customFieldsFns
	return p
}

func NewFirstBuilder(tableConfig TableConfig, builderFns ...SelectBuilderFn) *FirstParam {
	return &FirstParam{
		_Table:     TableFn(func() TableConfig { return tableConfig }),
		_Fields:    make(Fields, 0),
		_log:       DefaultLog,
		builderFns: builderFns,
	}
}

func (p *FirstParam) SetLog(log LogI) *FirstParam {
	p._log = log
	return p
}

func (p *FirstParam) WithCacheDuration(duration time.Duration) *FirstParam {
	p.context = WithCacheDuration(p.context, duration)
	return p
}

func (p *FirstParam) AppendFields(fields ...*Field) *FirstParam {
	p._Fields.Append(fields...)
	return p
}

func (p *FirstParam) WithHandler(handler Handler) *FirstParam {
	p.handler = handler
	return p
}
func (p *FirstParam) WithBuilderFns(builderFns ...SelectBuilderFn) *FirstParam {
	if len(p.builderFns) == 0 {
		p.builderFns = SelectBuilderFns{}
	}
	p.builderFns = append(p.builderFns, builderFns...)
	return p
}

// GetTable 获取表配置信息，增加获取表配置信息方法，方便在xxxBuilderFns 中能获取到表配置信息
func (p FirstParam) GetTable() TableConfig {
	return p._Table.TableConfig()
}

func (p *FirstParam) ApplyCustomFn(customFn CustomFnFirstParam) *FirstParam {
	if customFn != nil {
		customFn(p)
	}
	return p
}

func (p FirstParam) ToSQL() (sql string, err error) {
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_SELECT, tableConfig, p.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	errWithMsg := fmt.Sprintf("FirstParam.ToSQL(),table:%s", tableConfig.Name)
	where, err := fs.Where()
	if err != nil {
		err = errors.Wrap(err, errWithMsg)
		return "", err
	}
	ds := Dialect.DialectWrapper().Select(fs.Select()...).
		From(tableConfig.AliasOrTableExpr()).
		Where(where...).
		Order(fs.Order()...).
		Limit(1)
	ds = p.builderFns.Apply(ds)
	sql, _, err = ds.ToSQL()
	if err != nil {
		err = errors.Wrap(err, errWithMsg)
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

func (p FirstParam) First(result any) (exists bool, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return false, err
	}
	handler := p.handler
	cacheDuration := GetCacheDuration(p.context)
	if cacheDuration > 0 {
		handler = _WithCache(handler)
	}
	exists, err = handler.First(p.context, sql, result)
	return exists, err
}

func (p FirstParam) FirstMustExists(result any) (err error) {
	exists, err := p.First(result)
	if err != nil {
		return err
	}
	if !exists {
		return ERROR_NOT_FOUND
	}
	return nil
}

type SelectBuilderFn func(ds *goqu.SelectDataset) *goqu.SelectDataset

type SelectBuilderFns []SelectBuilderFn

func (fns SelectBuilderFns) Apply(ds *goqu.SelectDataset) *goqu.SelectDataset {
	for _, fn := range fns {
		ds = fn(ds)
	}
	return ds
}

type ListParam struct {
	_Table          TableI
	_Fields         Fields
	_log            LogI
	handler         Handler // 支持事务句柄
	builderFns      SelectBuilderFns
	context         context.Context
	customFieldsFns CustomFieldsFns // 定制化字段处理函数，可用于局部封装字段处理逻辑，比如通用模型中，用于设置查询字段的别名

}

func (p *ListParam) WithContext(ctx context.Context) *ListParam {
	p.context = ctx
	return p
}
func (p *ListParam) WithCustomFieldsFn(customFieldsFns ...CustomFieldsFn) *ListParam {
	p.customFieldsFns = customFieldsFns
	return p
}

func (p *ListParam) SetLog(log LogI) ListParam {
	p._log = log
	return *p
}
func (p *ListParam) WithHandler(handler Handler) *ListParam {
	p.handler = handler
	return p
}
func (p *ListParam) WithCacheDuration(duration time.Duration) *ListParam {
	p.context = WithCacheDuration(p.context, duration)
	return p
}
func (p *ListParam) WithBuilderFns(builderFns ...SelectBuilderFn) *ListParam {
	if len(p.builderFns) == 0 {
		p.builderFns = SelectBuilderFns{}
	}
	p.builderFns = append(p.builderFns, builderFns...)
	return p
}

func (p *ListParam) AppendFields(fields ...*Field) *ListParam {
	p._Fields.Append(fields...)
	return p
}

func (p *ListParam) ApplyCustomFn(customFn CustomFnListParam) *ListParam {
	if customFn != nil {
		customFn(p)
	}
	return p
}
func NewListBuilder(tableConfig TableConfig, builderFns ...SelectBuilderFn) *ListParam {
	return &ListParam{
		_Table:     TableFn(func() TableConfig { return tableConfig }),
		_log:       DefaultLog,
		builderFns: builderFns,
	}
}
func (p ListParam) GetTable() TableConfig {
	return p._Table.TableConfig()
}
func (p ListParam) ToSQL() (sql string, err error) {
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_SELECT, tableConfig, p.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	errWithMsg := fmt.Sprintf("ListParam.ToSQL(),table:%s", tableConfig.Name)
	where, err := fs.Where()
	if err != nil {
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}
	pageIndex, pageSize := fs.Pagination()
	ofsset := max(pageIndex*pageSize, 0)

	selec := fs.Select()
	order := fs.Order()
	ds := Dialect.DialectWrapper().Select(selec...).
		From(tableConfig.AliasOrTableExpr()).
		Where(where...).
		Order(order...)

	if pageSize > 0 {
		ds = ds.Offset(uint(ofsset)).Limit(uint(pageSize))
	}
	ds = p.builderFns.Apply(ds)
	sql, _, err = ds.ToSQL()
	if err != nil {
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

// Deprecated: 已废弃,请使用List
func (p ListParam) Query(result any) (err error) {
	return p.List(result)
}
func (p ListParam) List(result any) (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	handler := p.handler
	cacheDuration := GetCacheDuration(p.context)
	if cacheDuration > 0 {
		handler = _WithCache(handler) // 启用缓存中间件
	}
	err = handler.Query(p.context, sql, result)
	if err != nil {
		return err
	}
	return nil
}

type LogI interface {
	Log(sql string, args ...any)
}

type ConsoleLog struct{}

func (log ConsoleLog) Log(sql string, args ...any) {
	fmt.Println("sql:", sql, "args:", args)
}

type EmptyLog struct{}

func (log EmptyLog) Log(sql string, args ...any) {

}

var DefaultLog = ConsoleLog{}

type ExistsParam struct {
	_Table                   TableI
	_Fields                  Fields
	_log                     LogI
	allowEmptyWhereCondition bool
	handler                  Handler // 支持事务句柄
	builderFns               SelectBuilderFns
	context                  context.Context
	customFieldsFns          CustomFieldsFns
}

func (p *ExistsParam) AppendFields(fields ...*Field) *ExistsParam {
	p._Fields.Append(fields...)
	return p
}
func (p *ExistsParam) SetLog(log LogI) ExistsParam {
	p._log = log
	return *p
}
func (p *ExistsParam) WithContext(ctx context.Context) *ExistsParam {
	p.context = ctx
	return p
}
func (p *ExistsParam) WithCustomFieldsFn(customFieldsFns ...CustomFieldsFn) *ExistsParam {
	p.customFieldsFns = customFieldsFns
	return p
}
func (p *ExistsParam) WithHandler(handler Handler) *ExistsParam {
	p.handler = handler
	return p
}
func (p *ExistsParam) WithAllowEmptyWhereCondition(allowEmptyWhereCondition bool) *ExistsParam {
	p.allowEmptyWhereCondition = allowEmptyWhereCondition
	return p
}

// WithBuilderFns 配置sql构建器
func (p *ExistsParam) WithBuilderFns(builderFns ...SelectBuilderFn) *ExistsParam {
	if len(p.builderFns) == 0 {
		p.builderFns = SelectBuilderFns{}
	}
	p.builderFns = append(p.builderFns, builderFns...)
	return p
}

func NewExistsBuilder(tableConfig TableConfig, builderFns ...SelectBuilderFn) *ExistsParam {
	return &ExistsParam{
		_Table:     TableFn(func() TableConfig { return tableConfig }),
		_log:       DefaultLog,
		builderFns: builderFns,
	}
}

func (p ExistsParam) GetTable() TableConfig {
	return p._Table.TableConfig()
}

func (p ExistsParam) ToSQL() (sql string, err error) {
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_SELECT, tableConfig, p.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	errWithMsg := fmt.Sprintf("ExistsParam.ToSQL(),table:%s", tableConfig.Name)
	//fs.SetSceneIfEmpty(SCENE_SQL_EXISTS) // 存在场景，和SCENE_SQL_SELECT场景不一样，在set中，这个exists 必须实时查询数据，另外部分查询条件也和查询数据场景不一致，所以独立分开处理
	/*
		* 2025-05-21 10:46 SCENE_SQL_EXISTS 场景不存在.
		1. exists 必须实时查询，解决方案不在构造sql语句，而是在existsHandler 中处理，明确说明不使用缓存才是好的解决方案
		2. 部分是否存在 的查询条件和 查询数据场景不一致，这种情况可以再构造ExistsParam时删除 预定义的SCENE_SQL_SELECT 场景条件，再手动添加条件,这样就可以再现有场景下处理特殊场景
		为什么要否定SCENE_SQL_EXISTS场景：
		1. 保持公共包的简洁性，简洁意味着易用
		2. 预设场景条件时 insert、update、select、delete 容易想到，exists 场景容易忽略，增加使用负担
		3. 历史项目中使用select场景，exists场景都没设置，这回导致包升级不兼容，会引起重大升级陷阱，不利于包迭代发展
		处理方案：
		暂时注销SCENE_SQL_EXISTS场景，后续确实需要exists场景，再重构SCENE_SQL_EXISTS场景处理逻辑
	*/

	where, err := fs.Where()
	if err != nil {
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}

	ds := Dialect.DialectWrapper().
		From(tableConfig.AliasOrTableExpr()).
		Where(where...).
		Limit(1)
	ds = p.builderFns.Apply(ds)
	ds = ds.Select(goqu.L("1").As("exists")) // 确保不会被 p.builderFns.Apply 覆盖

	sql, _, err = ds.ToSQL()
	if err != nil {
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	if len(where) == 0 && !p.allowEmptyWhereCondition { // 默认where 条件不能为空，先写日志，再返回错误，方便用户查看SQL语句
		err = errors.Errorf("exists sql must have where condition")
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}
	return sql, nil
}

func (p ExistsParam) Exists() (exists bool, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return false, err
	}
	// 删除SCENE_SQL_EXISTS场景后需要确保确保exitsHandler 不会走缓存
	existsHandler := WithSingleflightDoOnce(p.handler.OriginalHandler()).Exists // 屏蔽缓存中间件，同时防止单实例并发问题
	return existsHandler(sql)
}

type TotalParam struct {
	_Table          TableI
	_Fields         Fields
	_log            LogI
	handler         Handler // 支持事务句柄
	builderFns      SelectBuilderFns
	context         context.Context
	customFieldsFns CustomFieldsFns
}

func NewTotalBuilder(tableConfig TableConfig, builderFns ...SelectBuilderFn) *TotalParam {
	return &TotalParam{
		_Table:     TableFn(func() TableConfig { return tableConfig }),
		_log:       DefaultLog,
		builderFns: builderFns,
	}
}
func (p *TotalParam) SetLog(log LogI) *TotalParam {
	p._log = log
	return p
}

func (p *TotalParam) WithContext(ctx context.Context) *TotalParam {
	p.context = ctx
	return p
}
func (p *TotalParam) WithCustomFieldsFn(customFieldsFns ...CustomFieldsFn) *TotalParam {
	p.customFieldsFns = customFieldsFns
	return p
}
func (p *TotalParam) WithHandler(handler Handler) *TotalParam {
	p.handler = handler
	return p
}

func (p *TotalParam) WithBuilderFns(builderFns ...SelectBuilderFn) *TotalParam {
	if len(p.builderFns) == 0 {
		p.builderFns = SelectBuilderFns{}
	}
	p.builderFns = append(p.builderFns, builderFns...)
	return p
}

func (p *TotalParam) AppendFields(fields ...*Field) *TotalParam {
	p._Fields.Append(fields...)
	return p
}

func (p TotalParam) GetTable() TableConfig {
	return p._Table.TableConfig()
}

func (p TotalParam) ToSQL() (sql string, err error) {
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_SELECT, tableConfig, p.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	errWithMsg := fmt.Sprintf("TotalParam.ToSQL(),table:%s", tableConfig.Name)
	where, err := fs.Where()
	if err != nil {
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}
	ds := Dialect.DialectWrapper().
		From(tableConfig.AliasOrTableExpr()).
		Where(where...)
	ds = p.builderFns.Apply(ds)
	ds = ds.Select(goqu.COUNT(goqu.Star()).As("count")) // 确保select 部分固定
	sql, _, err = ds.ToSQL()
	if err != nil {
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

func (p TotalParam) Count() (total int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return -1, err
	}
	return p.handler.Count(sql)
}

type PaginationParam struct {
	_Table          TableI
	_Fields         Fields
	handler         Handler // 支持事务句柄
	builderFns      SelectBuilderFns
	context         context.Context
	customFieldsFns CustomFieldsFns
}

func (p *PaginationParam) AppendFields(fields ...*Field) *PaginationParam {
	p._Fields.Append(fields...)
	return p
}

func NewPaginationBuilder(tableConfig TableConfig) *PaginationParam {
	return &PaginationParam{
		_Table: TableFn(func() TableConfig { return tableConfig }),
	}
}
func (p *PaginationParam) WithContext(ctx context.Context) *PaginationParam {
	p.context = ctx
	return p
}
func (p *PaginationParam) WithCustomFieldsFn(customFieldsFns CustomFieldsFns) *PaginationParam {
	p.customFieldsFns = customFieldsFns
	return p
}
func (p *PaginationParam) WithHandler(handler Handler) *PaginationParam {
	p.handler = handler
	return p
}

func (p *PaginationParam) WithCacheDuration(duration time.Duration) *PaginationParam {
	p.context = WithCacheDuration(p.context, duration)
	return p
}

func (p *PaginationParam) WithBuilderFns(builderFns ...SelectBuilderFn) *PaginationParam {
	if len(p.builderFns) == 0 {
		p.builderFns = SelectBuilderFns{}
	}
	p.builderFns = append(p.builderFns, builderFns...)
	return p
}

func (p PaginationParam) GetTable() TableConfig {
	return p._Table.TableConfig()
}
func (p *PaginationParam) ApplyCustomFn(customFn CustomFnPaginationParam) *PaginationParam {
	if customFn != nil {
		customFn(p)
	}
	return p
}

func (p PaginationParam) ToSQL() (totalSql string, listSql string, err error) {
	table := p.GetTable()
	totalSql, err = NewTotalBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).WithBuilderFns(p.builderFns...).ToSQL()
	if err != nil {
		return "", "", err
	}
	listSql, err = NewListBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).WithBuilderFns(p.builderFns...).ToSQL()
	if err != nil {
		return "", "", err
	}
	return totalSql, listSql, nil
}

func (p PaginationParam) paginationHandler(totalSql string, listSql string, result any) (count int64, err error) {
	handler := p.handler
	cacheDuration := GetCacheDuration(p.context)
	if cacheDuration > 0 {
		handler = _WithCache(handler)
	}

	count, err = handler.Count(totalSql)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}
	err = p.handler.Query(p.context, listSql, result)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (p PaginationParam) Pagination(result any) (count int64, err error) {
	totalSql, listSql, err := p.ToSQL()
	if err != nil {
		return 0, err
	}

	index, size := p._Fields.Pagination()
	if index == 0 && size == 0 {
		err = errors.Errorf("pagination size required,got sql:%s", listSql)
		return 0, err
	}

	return p.paginationHandler(totalSql, listSql, result)
}

type SetPolicy string

const (
	SetPolicy_only_Insert      SetPolicy = "onlyInsert"       //只新增说明使用最早数据
	SetPolicy_only_Update      SetPolicy = "onlyUpdate"       //只更新说明不存在时不处理
	SetPolicy_Insert_or_Update SetPolicy = "insert_or_Update" //不存在新增,存在更新，使用最新数据覆盖
)

type SetParam struct {
	_Table                TableI
	_Fields               Fields
	handler               Handler   // 支持事务句柄
	setPolicy             SetPolicy // 更新策略,默认根据主键判断是否需要更新
	_triggerInsertedEvent EventInsertTrigger
	_triggerUpdatedEvent  EventUpdateTrigger
	context               context.Context
	customFieldsFns       CustomFieldsFns
}

func (p *SetParam) AppendFields(fields ...*Field) *SetParam {
	p._Fields.Append(fields...)
	return p
}

func NewSetBuilder(tableConfig TableConfig) *SetParam {
	return &SetParam{
		_Table: TableFn(func() TableConfig { return tableConfig }),
	}
}

func (p *SetParam) WithPolicy(policy SetPolicy) *SetParam {
	p.setPolicy = policy
	return p
}

func (p *SetParam) WithContext(ctx context.Context) *SetParam {
	p.context = ctx
	return p
}

func (p *SetParam) WithCustomFieldsFn(customFieldsFns ...CustomFieldsFn) *SetParam {
	p.customFieldsFns = customFieldsFns
	return p
}

func (p *SetParam) WithTriggerEvent(triggerInsertdEvent EventInsertTrigger, triggerUpdateEvent EventUpdateTrigger) *SetParam {
	p._triggerInsertedEvent = triggerInsertdEvent
	p._triggerUpdatedEvent = triggerUpdateEvent
	return p
}

func (p *SetParam) getEventHandler() (triggerInsertdEvent EventInsertTrigger, triggerUpdateEvent EventUpdateTrigger) {
	triggerInsertdEvent = p._triggerInsertedEvent
	if triggerInsertdEvent == nil {
		triggerInsertdEvent = func(lastInsertId uint64, rowsAffected int64) (err error) { return nil }
	}
	triggerUpdateEvent = p._triggerUpdatedEvent
	if triggerUpdateEvent == nil {
		triggerUpdateEvent = func(rowsAffected int64) (err error) { return nil }
	}
	return triggerInsertdEvent, triggerUpdateEvent
}

func (p *SetParam) WithHandler(handler Handler) *SetParam {
	p.handler = handler

	return p
}

func (p SetParam) GetTable() TableConfig {
	return p._Table.TableConfig()
}

// ToSQL 一次生成 查询、新增、修改 sql,若查询后记录存在,并且需要根据数据库记录值修改数据,则可以重新赋值后生成sql
func (p SetParam) ToSQL() (existsSql string, insertSql string, updateSql string, err error) {
	table := p.GetTable()
	existsSql, err = NewExistsBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).ToSQL() // 有些根据场景设置 如枚举值 ""，所有需要复制
	if err != nil {
		return "", "", "", err
	}
	insertSql, err = NewInsertBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", "", err
	}
	updateSql, err = NewUpdateBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", "", err
	}
	return existsSql, insertSql, updateSql, nil
}

func (p SetParam) Set() (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	// 因为自带 exists 查询，所以不需要校验唯一索引了，否则 存在 table.CheckUniqueIndex 就会报错，达不到update 效果
	// table := p.GetTable()
	// err = table.CheckUniqueIndex(p._Fields...)
	// if err != nil {
	// 	return false, 0, 0, err
	// }
	existsSql, insertSql, updateSql, err := p.ToSQL()
	if err != nil {
		return false, 0, 0, err
	}

	existsHandler := WithSingleflightDoOnce(p.handler.OriginalHandler()).Exists // 屏蔽缓存中间件，同时防止单实例并发问题
	// insertWithLastIdHandler := p.handler.InsertWithLastIdHandler
	// execWithRowsAffectedHandler := p.handler.ExecWithRowsAffected
	triggerInsertdEvent, triggerUpdateEvent := p.getEventHandler()
	withInsertEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		triggerInsertdEvent(event.LastInsertId, event.RowsAffected)
	})
	withUpdateEventHandler := WithTriggerAsyncEvent(p.handler, func(event *Event) {
		triggerUpdateEvent(event.RowsAffected)
	})

	exists, err := existsHandler(existsSql)
	if err != nil {
		return false, 0, 0, err
	}
	isInsert = !exists
	switch p.setPolicy {
	case SetPolicy_only_Insert: // 只新增说明使用最早数据
		if !exists {
			lastInsertId, rowsAffected, err = withInsertEventHandler.InsertWithLastId(insertSql)
			return isInsert, lastInsertId, rowsAffected, err
		}
	case SetPolicy_only_Update: // 只更新说明不存在时不处理
		if exists {
			rowsAffected, err = withUpdateEventHandler.ExecWithRowsAffected(updateSql)
			return isInsert, lastInsertId, rowsAffected, err
		}
	default: // 默认执行 SetPolicy_Insert_or_Update 策略
		if exists {
			rowsAffected, err = withUpdateEventHandler.ExecWithRowsAffected(updateSql)
		} else {
			lastInsertId, rowsAffected, err = withInsertEventHandler.InsertWithLastId(insertSql)
		}
	}
	return isInsert, lastInsertId, rowsAffected, err
}

func MergeData(dataFns ...func() (any, error)) (map[string]any, error) {
	newData := map[string]any{}
	for _, dataFn := range dataFns {
		if IsNil(dataFn) {
			continue
		}
		data, err := dataFn()
		if IsErrorValueNil(err) {
			err = nil // 消除error
		}
		if err != nil {
			return newData, err
		}
		subMap, err := dataAny2Map(data)
		if err != nil {
			return nil, err
		}
		for k, v := range subMap {
			newData[k] = v
		}
	}
	return newData, nil
}

// dataAny2Map data 从any 格式转为map格式
func dataAny2Map(data any) (newData map[string]any, err error) {
	newData = map[string]any{}
	if IsNil(data) {
		return nil, nil
	}
	rv := reflect.Indirect(reflect.ValueOf(data))
	switch rv.Kind() {
	case reflect.Map:
		keys := rv.MapKeys()
		for _, key := range keys {
			newData[key.String()] = rv.MapIndex(key).Interface()
		}
	case reflect.Struct:
		r, err := exp.NewRecordFromStruct(rv.Interface(), false, true)
		if err != nil {
			return nil, err
		}
		for k, v := range r {
			newData[k] = v
		}
	default:
		return nil, errors.Errorf("unsupported update interface type %+v,got:%+v", rv.Type(), data)
	}
	return newData, nil
}
