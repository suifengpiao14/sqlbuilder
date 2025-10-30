package sqlbuilder

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/memorytable"
	_ "gorm.io/driver/mysql"
	_ "gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type BuilderFn func() Builder

type Builder struct {
	//handler Handler //2025-06-27  废弃，改为直接使用table.handler 虽然有不兼容场景，但是错误非常明显，改动不大，没有bug风险
	table TableConfig
}

// Deprecated: 废弃，直接获取 NewBuilder 即可

func NewGormBuilder(table TableConfig, getDB func() *gorm.DB) Builder {
	handler := NewGormHandler(getDB)
	table = table.WithHandler(handler)
	return NewBuilder(table)
}

func NewBuilder(table TableConfig) Builder { // 因为 WithHandler 需要复制，所以这里统一不返回地址值
	return Builder{table: table}
}

func (b Builder) WithHandler(handler Handler) Builder { // transaction 时候需要重新设置handler
	table := b.table.WithHandler(handler)
	return Builder{table: table}
}

func (b Builder) Handler() (handler Handler) { // 提供给外部使用
	return b.table.GetHandler()
}

func (b Builder) TotalParam(fs Fields) *TotalParam {
	p := NewTotalBuilder(b.table).AppendFields(fs...)
	return p

}
func (b Builder) ListParam(fs Fields) *ListParam {
	p := NewListBuilder(b.table).AppendFields(fs...)
	return p

}

func (b Builder) PaginationParam(fs Fields) *PaginationParam {
	p := NewPaginationBuilder(b.table).AppendFields(fs...)
	return p
}
func (b Builder) FirstParam(fs Fields) *FirstParam {
	p := NewFirstBuilder(b.table).AppendFields(fs...)
	return p
}
func (b Builder) InsertParam(fs Fields) *InsertParam {
	p := NewInsertBuilder(b.table).AppendFields(fs...)
	return p
}
func (b Builder) BatchInsertParam(fss []Fields) *BatchInsertParam {
	p := NewBatchInsertBuilder(b.table).AppendFields(fss...)
	return p

}
func (b Builder) UpdateParam(fs Fields) *UpdateParam {
	p := NewUpdateBuilder(b.table).AppendFields(fs...)
	return p
}
func (b Builder) DeleteParam(fs Fields) *DeleteParam {
	p := NewDeleteBuilder(b.table).AppendFields(fs...)
	return p
}

func (b Builder) ExistsParam(fs Fields) *ExistsParam {
	p := NewExistsBuilder(b.table).AppendFields(fs...)
	return p
}
func (b Builder) SetParam(fs Fields) *SetParam {
	p := NewSetBuilder(b.table).AppendFields(fs...)
	return p
}

/*
// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) Count(fields Fields) (count int64, err error) {
		return b.TotalParam(fields).Count()
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) List(result any, fields Fields) (err error) {
		return b.ListParam(fields).Query(result)
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) Pagination(result any, fields Fields) (count int64, err error) {
		return b.PaginationParam(fields).Pagination(result)
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) First(result any, fields Fields) (exists bool, err error) {
		return b.FirstParam(fields).First(result)
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) Insert(fields Fields) (err error) {
		return b.InsertParam(fields).Exec()
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) BatchInsert(fields []Fields) (err error) {
		return b.BatchInsertParam(fields).Exec()
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) Update(fields Fields) (err error) {
		return b.UpdateParam(fields).Exec()
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) Delete(fields Fields) (err error) {
		return b.DeleteParam(fields).Exec()
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) Exists(fields Fields) (exists bool, err error) {
		return b.ExistsParam(fields).Exists()
	}

// Deprecated: 废弃，使用 xxxParam 直接调用即可

	func (b Builder) Set(fields Fields) (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
		return b.SetParam(fields).Set()
	}
*/
type Driver string

func (d Driver) String() string {
	return string(d)
}

func (d Driver) IsSame(target Driver) bool {
	return strings.EqualFold(d.String(), target.String())
}

func (d Driver) GoquDialect() goqu.DialectWrapper {
	return goqu.Dialect(d.String())
}

func (d Driver) EscapeString(val string) string {
	switch strings.ToLower(d.String()) {
	case strings.ToLower(Driver_mysql.String()):
		return MysqlEscapeString(val)
	case strings.ToLower(Driver_sqlite3.String()):
		return SQLite3EscapeString(val)
	}
	return val
}

type Expressions = []goqu.Expression

var ErrEmptyWhere = errors.New("error  empty where")

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

// Deprecated: 废弃，使用 Driver.GoquDialect 代替
type DialectWrapper struct {
	dialect string
}

// Deprecated: 废弃，使用 Driver.GoquDialect 代替
func (d DialectWrapper) Dialect() string {
	return d.dialect
}

// Deprecated: 废弃，使用 Driver.GoquDialect 代替
func (d DialectWrapper) DialectWrapper() goqu.DialectWrapper {
	return goqu.Dialect(d.dialect)
}

// Deprecated: 废弃，使用 Driver.EscapeString 代替
func (d DialectWrapper) EscapeString(val string) string {
	switch strings.ToLower(d.dialect) {
	case strings.ToLower(Driver_mysql.String()):
		return MysqlEscapeString(val)
	case strings.ToLower(Driver_sqlite3.String()):
		return SQLite3EscapeString(val)
	}
	return val
}

// Deprecated: 废弃，使用 Driver
func (d DialectWrapper) IsMysql() bool {
	return strings.EqualFold(d.dialect, Driver_mysql.String())
}

// Deprecated: 废弃，使用 Driver
func (d DialectWrapper) IsSQLite3() bool {
	return strings.EqualFold(d.dialect, Driver_sqlite3.String())
}

// Deprecated: 废弃，使用 Driver
func NewDialect(dialect string) DialectWrapper {
	return DialectWrapper{
		dialect: dialect,
	}
}

// Deprecated: 使用 Driver 代替
var Dialect = NewDialect(Driver_sqlite3.String())

// Deprecated: 使用 Driver 代替
var Dialect_Mysql = NewDialect(Driver_mysql.String())

// Deprecated: 废弃，使用 Driver.EscapeString 代替
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
	//SCENE_SQL_VIEW Scene = "view" // 视图查询，就是select查询，暂时废弃，视图查询，不能作为一种场景，污染场景定义
	//SCENE_SQL_INCREASE Scene = "increse" // 字段递增 2025-01-25 暂时废弃，递增递减，不能作为一种场景，污染场景定义
	//SCENE_SQL_DECREASE Scene = "decrese" // 字段递减 2025-01-25 暂时废弃，递增递减，不能作为一种场景，污染场景定义
	SCENE_SQL_FINAL Scene = "final" // 最终状态(所有场景执行完成后再执行的场景 ,有时需要清除公共库设置的所有场景，只有在这里清除最保险)
)

// 操作数据库场景
// var SCENE_Commands = Scenes{SCENE_SQL_INSERT, SCENE_SQL_UPDATE, SCENE_SQL_DELETE, SCENE_SQL_INCREASE, SCENE_SQL_DECREASE}
var SCENE_Commands = Scenes{SCENE_SQL_INSERT, SCENE_SQL_UPDATE, SCENE_SQL_DELETE}

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
	SQLParam[InsertParam]
	insertIgnore        bool
	_triggerInsertEvent EventInsertTrigger
}

func (p *InsertParam) WithTriggerEvent(triggerInsertEvent EventInsertTrigger) *InsertParam {
	p._triggerInsertEvent = triggerInsertEvent
	return p
}

func (p *InsertParam) WithInsertIgnore(insertIgnore bool) *InsertParam {
	p.insertIgnore = insertIgnore
	return p
}
func (p *InsertParam) getEventHandler() (triggerInsertEvent EventInsertTrigger) {
	triggerInsertEvent = p._triggerInsertEvent
	if triggerInsertEvent == nil {
		triggerInsertEvent = func(lastInsertId uint64, rowsAffected int64) (err error) { return }
	}
	return triggerInsertEvent
}

func NewInsertBuilder(tableConfig TableConfig) *InsertParam {
	p := &InsertParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

type CustomFnInsertParam = CustomFn[InsertParam]
type CustomFnInsertParams = CustomFns[InsertParam]

func (p *InsertParam) ApplyCustomFn(customFns ...CustomFnInsertParam) *InsertParam {
	p = CustomFns[InsertParam](customFns).Apply(p)
	return p
}

func (p InsertParam) ToSQL() (sql string, err error) {
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
	ds := p.GetGoquDialect().Insert(tableConfig.Name).Rows(rowData)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p.insertIgnore {
		// 替换前缀为 INSERT IGNORE
		sql = replaceInsertWithInsertIgnore(sql)
	}
	p.Log(sql)
	return sql, nil
}

func replaceInsertWithInsertIgnore(sql string) string {
	if len(sql) >= 6 && sql[:6] == "INSERT" {
		return "INSERT IGNORE" + sql[6:]
	}
	return sql
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
	withEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = p.getEventHandler()(event.LastInsertId, event.RowsAffected)
		if err != nil {
			p.Log(sql, err)
		}
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
	withEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = p.getEventHandler()(event.LastInsertId, event.RowsAffected)
		if err != nil {
			p.Log(sql, err)
		}
	})
	return withEventHandler.InsertWithLastId(sql)
}

type BatchInsertParam struct {
	rowFields           []Fields
	_triggerInsertEvent EventInsertTrigger
	SQLParam[BatchInsertParam]
}

func (p BatchInsertParam) Fields() []Fields {
	return p.rowFields
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
	p := &BatchInsertParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

func (p *BatchInsertParam) AppendFields(fields ...Fields) *BatchInsertParam {
	if p.rowFields == nil {
		p.rowFields = make([]Fields, 0)
	}
	p.rowFields = append(p.rowFields, fields...)
	return p
}

var ErrBatchInsertDataIsNil = errors.New("batch insert err: data is nil")
var ErrNotFound = errors.New("not found record")
var ErrWithSQL = false // 正式环境关闭,开发调试可以开启

// Deprecated: use ErrNotFound instead
var ERROR_NOT_FOUND = ErrNotFound

type CustomFnBatchInsertParam = CustomFn[BatchInsertParam]
type CustomFnBatchInsertParams = CustomFns[BatchInsertParam]

func (p *BatchInsertParam) ApplyCustomFn(customFns ...CustomFnBatchInsertParam) *BatchInsertParam {
	p = CustomFns[BatchInsertParam](customFns).Apply(p)
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
		return "", ErrBatchInsertDataIsNil
	}
	ds := is.GetGoquDialect().Insert(tableConfig.Name).Rows(data...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	is.Log(sql)
	return sql, nil
}
func (p BatchInsertParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}

	withEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = p.getEventHandler()(event.LastInsertId, event.RowsAffected)
		if err != nil {
			p.Log(sql, err)
		}
	})
	_, _, err = withEventHandler.InsertWithLastId(sql)
	return err
}
func (p BatchInsertParam) InsertWithLastId() (lastInsertId uint64, rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return 0, 0, err
	}
	withEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = p.getEventHandler()(event.LastInsertId, event.RowsAffected)
		if err != nil {
			p.Log(sql, err)
		}
	})
	return withEventHandler.InsertWithLastId(sql)
}

type DeleteParam struct {
	_triggerDeletedEvent EventDeletedTrigger
	SQLParam[DeleteParam]
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

func NewDeleteBuilder(tableConfig TableConfig) *DeleteParam {
	p := &DeleteParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

type CustomFnDeleteParam = CustomFn[DeleteParam]
type CustomFnDeleteParams = CustomFns[DeleteParam]

func (p *DeleteParam) ApplyCustomFn(customFns ...CustomFnDeleteParam) *DeleteParam {
	p = CustomFns[DeleteParam](customFns).Apply(p)
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
	ds := p.GetGoquDialect().Update(tableConfig.Name).Set(data).Where(where...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		err = errors.Wrap(err, "build delete sql error")
		return "", err
	}
	p.Log(sql)
	return sql, nil
}
func (p DeleteParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	withEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = p.getEventHandler()(event.RowsAffected)
		if err != nil {
			p.Log(sql, err)
		}
	})
	_, err = withEventHandler.ExecWithRowsAffected(sql)
	return err
}
func (p DeleteParam) Delete() (rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return rowsAffected, err
	}
	withEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = p.getEventHandler()(event.RowsAffected)
		if err != nil {
			p.Log(sql, err)
		}
	})
	rowsAffected, err = withEventHandler.ExecWithRowsAffected(sql)
	return rowsAffected, err
}

// deprecated use Delete instead
func (p DeleteParam) ExecWithRowsAffected() (rowsAffected int64, err error) {
	return p.Delete()
}

type UpdateParam struct {
	mustExists           bool
	_triggerUpdatedEvent EventUpdateTrigger
	SQLParam[UpdateParam]
}

func (p *UpdateParam) WithMustExists(mustExists bool) *UpdateParam {
	p.mustExists = mustExists
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

func NewUpdateBuilder(tableConfig TableConfig) *UpdateParam {
	p := &UpdateParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

type CustomFnUpdateParam = CustomFn[UpdateParam]
type CustomFnUpdateParams = CustomFns[UpdateParam]

func (p *UpdateParam) ApplyCustomFn(customFns ...CustomFnUpdateParam) *UpdateParam {
	p = CustomFns[UpdateParam](customFns).Apply(p)
	return p
}

func (p UpdateParam) ToSQL() (sql string, err error) {
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_UPDATE, tableConfig, p.customFieldsFns) // 使用复制变量,后续针对场景的特殊化处理不会影响原始变量
	data, err := fs.Data(layer_order...)
	if err != nil {
		return "", err
	}

	where, err := fs.Where()
	if err != nil {
		return "", err
	}
	if len(where) == 0 {
		err = errors.WithMessage(ErrEmptyWhere, "update must have where condition")
		return "", err
	}
	limit := fs.Limit()

	ds := p.GetGoquDialect().Update(tableConfig.Name).Set(data).Where(where...).Order(fs.Order()...)
	if limit > 0 {
		ds = ds.Limit(limit)
	}

	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	p.Log(sql)
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
	_, err = p.Update()
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
	if p.mustExists {
		cp := p._Fields.Copy()
		existsParam := NewExistsBuilder(p.GetTable()).AppendFields(cp...)
		exists, err := existsParam.Exists()
		if err != nil {
			return 0, err
		}
		if !exists {
			wherePression, _ := existsParam._Fields.Where()
			existsSql := fmt.Sprintf("sql where :%s", Expression2StringWithDriver(Driver(p.GetTable().GetHandler().GetDialector()), wherePression...))
			if ErrWithSQL {
				existsSql, _ = existsParam.ToSQL()
				existsSql = fmt.Sprintf("sql:%s", existsSql)
			}
			err = errors.WithMessagef(ErrNotFound, " UpdateParam.mustExists==true %s", existsSql)
			return 0, err
		}
	}
	withEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = p.getEventHandler()(event.RowsAffected)
		if err != nil {
			p.Log(sql, err)
		}
	})
	rowsAffected, err = withEventHandler.ExecWithRowsAffected(sql)
	return rowsAffected, err
}

// Deprecated :已废弃,请使用p.WithMustExists(true).Update()(UpdateMustExists 这个名字很难想起来,所以改为配置模式，另外也减少重复代码)
func (p UpdateParam) UpdateMustExists() (rowsAffected int64, err error) {
	p.WithMustExists(true)
	return p.Update()
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
	builderFns SelectBuilderFns
	SQLParam[FirstParam]
}

func NewFirstBuilder(tableConfig TableConfig) *FirstParam {
	p := &FirstParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

func (p *FirstParam) WithCacheDuration(duration time.Duration) *FirstParam {
	p.context = WithCacheDuration(p.context, duration)
	return p
}

func (p *FirstParam) WithBuilderFns(builderFns ...SelectBuilderFn) *FirstParam {
	if len(p.builderFns) == 0 {
		p.builderFns = SelectBuilderFns{}
	}
	p.builderFns = append(p.builderFns, builderFns...)
	return p
}

type CustomFnFirstParam = CustomFn[FirstParam]
type CustomFnFirstParams = CustomFns[FirstParam]

func (p *FirstParam) ApplyCustomFn(customFns ...CustomFnFirstParam) *FirstParam {
	p = CustomFns[FirstParam](customFns).Apply(p)
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
	ds := p.GetGoquDialect().Select(p.getSelectColumns(fs)...).
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
	p.Log(sql)
	return sql, nil
}

func (p FirstParam) First(result any) (exists bool, err error) {
	p.resultDst = result
	sql, err := p.ToSQL()
	if err != nil {
		return false, err
	}
	handler := p.GetHandlerWithInitTable()
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
		return ErrNotFound
	}
	return nil
}

// ds 传入传出是2个不同的实例，函数中需要明确赋值给返回的实例
type SelectBuilderFn func(ds *goqu.SelectDataset) *goqu.SelectDataset

type SelectBuilderFns []SelectBuilderFn

func (fns SelectBuilderFns) Apply(ds *goqu.SelectDataset) *goqu.SelectDataset {
	for _, fn := range fns {
		ds = fn(ds)
	}
	return ds
}

type ListParam struct {
	builderFns SelectBuilderFns
	SQLParam[ListParam]
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

type CustomFnListParam = CustomFn[ListParam]
type CustomFnListParams = CustomFns[ListParam]

func (p *ListParam) ApplyCustomFn(customFns ...CustomFnListParam) *ListParam {
	p = CustomFns[ListParam](customFns).Apply(p)
	return p
}
func NewListBuilder(tableConfig TableConfig) *ListParam {
	p := &ListParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

func (p ListParam) makeSelectDataset() (ds *goqu.SelectDataset, err error) {
	tableConfig := p.GetTable()
	fs := p._Fields.Builder(p.context, SCENE_SQL_SELECT, tableConfig, p.customFieldsFns) // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	errWithMsg := fmt.Sprintf("ListParam.ToSQL(),table:%s", tableConfig.Name)
	where, err := fs.Where()
	if err != nil {
		err = errors.WithMessage(err, errWithMsg)
		return nil, err
	}
	pageIndex, pageSize := fs.Pagination()
	ofsset := max(pageIndex*pageSize, 0)

	selec := p.getSelectColumns(fs)
	order := fs.Order()
	if len(order) == 0 { // 没有排序字段,则默认按主键降序排列
		table := p.GetTable()
		primary, exists := table.Indexs.GetPrimary()
		if exists {
			for _, columnName := range primary.ColumnNames(table) {
				fullName := fmt.Sprintf("%s.%s", table.BaseName(), columnName) // 增加表名，避免jion查询多表字段冲突
				subOrder := goqu.I(fullName).Asc()
				order = append(order, subOrder)
			}
		}
	}

	ds = p.GetGoquDialect().Select(selec...).
		From(tableConfig.AliasOrTableExpr()).
		Where(where...).
		Order(order...)

	if pageSize > 0 {
		ds = ds.Offset(uint(ofsset)).Limit(uint(pageSize))
	}
	ds = p.builderFns.Apply(ds)
	return ds, nil
}
func (p ListParam) ToSQL() (sql string, err error) {
	ds, err := p.makeSelectDataset()
	if err != nil {
		return "", err
	}
	sql, _, err = ds.ToSQL()
	if err != nil {
		tableConfig := p.GetTable()
		errWithMsg := fmt.Sprintf("ListParam.ToSQL(),table:%s", tableConfig.Name)
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}
	p.Log(sql)
	return sql, nil
}

// Deprecated: 已废弃,请使用List
func (p ListParam) Query(result any) (err error) {
	return p.List(result)
}
func (p ListParam) List(result any) (err error) {
	p.resultDst = result
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	handler := p.GetHandlerWithInitTable()
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
	//allowEmptyWhereCondition bool // 如果应用中确实容许where 条件为空，请使用 where 1=1 替换
	builderFns SelectBuilderFns
	SQLParam[ExistsParam]
}

// func (p *ExistsParam) WithAllowEmptyWhereCondition(allowEmptyWhereCondition bool) *ExistsParam {
// 	p.allowEmptyWhereCondition = allowEmptyWhereCondition
// 	return p
// }

// WithBuilderFns 配置sql构建器
func (p *ExistsParam) WithBuilderFns(builderFns ...SelectBuilderFn) *ExistsParam {
	if len(p.builderFns) == 0 {
		p.builderFns = SelectBuilderFns{}
	}
	p.builderFns = append(p.builderFns, builderFns...)
	return p
}

func NewExistsBuilder(tableConfig TableConfig) *ExistsParam {
	p := &ExistsParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

type CustomFnExistsParam = CustomFn[ExistsParam]
type CustomFnExistsParams = CustomFns[ExistsParam]

func (p *ExistsParam) ApplyCustomFn(customFns ...CustomFnExistsParam) *ExistsParam {
	p = CustomFns[ExistsParam](customFns).Apply(p)
	return p
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

	ds := p.GetGoquDialect().
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
	p.Log(sql)
	if len(where) == 0 { // 默认where 条件不能为空，先写日志，再返回错误，方便用户查看SQL语句
		err = ErrEmptyWhere
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
	existsHandler := WithSingleflightDoOnce(p.GetHandlerWithInitTable().OriginalHandler()).Exists // 屏蔽缓存中间件，同时防止单实例并发问题
	return existsHandler(sql)
}

type TotalParam struct {
	builderFns SelectBuilderFns
	SQLParam[TotalParam]
}

func NewTotalBuilder(tableConfig TableConfig) *TotalParam {
	p := &TotalParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

type CustomFnTotalParam = CustomFn[TotalParam]
type CustomFnTotalParams = CustomFns[TotalParam]

func (p *TotalParam) ApplyCustomFn(customFns ...CustomFnTotalParam) *TotalParam {
	p = CustomFns[TotalParam](customFns).Apply(p)
	return p
}

type CustomFn[P any] func(p *P)

func (fn CustomFn[P]) Apply(p *P) *P {
	if fn != nil {
		fn(p)
	}
	return p
}

type CustomFns[P any] []CustomFn[P]

func (fns CustomFns[P]) Apply(p *P) *P {
	for _, fn := range fns {
		p = fn.Apply(p)
	}
	return p
}
func (fns CustomFns[P]) Append(fn CustomFn[P]) CustomFns[P] {
	if fns == nil {
		fns = make([]CustomFn[P], 0)
	}
	return append(fns, fn)
}

func (p *TotalParam) WithBuilderFns(builderFns ...SelectBuilderFn) *TotalParam {
	if len(p.builderFns) == 0 {
		p.builderFns = SelectBuilderFns{}
	}
	p.builderFns = append(p.builderFns, builderFns...)
	return p
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
	ds := p.GetGoquDialect().
		From(tableConfig.AliasOrTableExpr()).
		Where(where...)
	ds = p.builderFns.Apply(ds)
	var countColumn any = goqu.Star()
	countColumns := fs.CountColumn()
	if len(countColumns) > 0 { // 使用自定义计数列
		countColumn = countColumns[0] // 此处只取第一个，设置多个，第二个及以后的计数列将被忽略
		if countColumnStr, ok := countColumn.(string); ok && countColumnStr == CountColumns_use_select_sql {
			/*
				实现如下案例
								SELECT COUNT(*) AS `count`
					FROM (
					    SELECT DISTINCT t_xian_yu_product_map.*
					    FROM t_xian_yu_product_map
					    LEFT JOIN t_xian_yu_product_scene_state
					        ON t_xian_yu_product_map.Fxy_spu_id = t_xian_yu_product_scene_state.Fxy_spu_id
					    WHERE t_xian_yu_product_map.Fdelete_flag = 0
					) AS sub;
			*/
			fs := p._Fields.RemovePagination() // 复制并移除分页信息
			subQuery, err := NewListBuilder(tableConfig).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(fs...).WithBuilderFns(p.builderFns...).makeSelectDataset()
			if err != nil {
				return "", err
			}
			ds = p.GetGoquDialect().From(subQuery.As("sub")) // 重新生成ds
			countColumn = goqu.Star()                        //设置常规化
		}
	}
	ds = ds.Select(goqu.COUNT(countColumn).As("count")) // 确保select 部分固定
	sql, _, err = ds.ToSQL()
	if err != nil {
		err = errors.WithMessage(err, errWithMsg)
		return "", err
	}
	p.Log(sql)
	return sql, nil
}

func (p TotalParam) Count() (total int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return -1, err
	}
	return p.GetHandlerWithInitTable().Count(sql)
}

type PaginationParam struct {
	builderFns SelectBuilderFns
	SQLParam[PaginationParam]
	countColumns []any
}

func NewPaginationBuilder(tableConfig TableConfig) *PaginationParam {
	p := &PaginationParam{
		countColumns: make([]any, 0),
	}
	p.SQLParam = NewSQLParam(p, tableConfig)
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

type CustomFnPaginationParam = CustomFn[PaginationParam]
type CustomFnPaginationParams = CustomFns[PaginationParam]

func (p *PaginationParam) ApplyCustomFn(customFns ...CustomFnPaginationParam) *PaginationParam {
	p = CustomFns[PaginationParam](customFns).Apply(p)
	return p
}

func (p PaginationParam) ToSQL() (totalSql string, listSql string, err error) {
	table := p.GetTable()
	totalSql, err = NewTotalBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).WithBuilderFns(p.builderFns...).ToSQL()
	if err != nil {
		return "", "", err
	}
	listSql, err = NewListBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).WithBuilderFns(p.builderFns...).WithResultDest(p.resultDst).ToSQL()
	if err != nil {
		return "", "", err
	}
	return totalSql, listSql, nil
}
func (p PaginationParam) getHandler() (handler Handler) {
	handler = p.GetHandlerWithInitTable()
	cacheDuration := GetCacheDuration(p.context)
	if cacheDuration > 0 {
		handler = _WithCache(handler)
	}
	return handler
}

func (p PaginationParam) paginationHandler(totalSql string, listSql string, result any) (count int64, err error) {
	handler := p.getHandler()
	count, err = handler.Count(totalSql)
	if err != nil {
		return 0, err
	}
	if count == 0 {
		return 0, nil
	}
	err = handler.Query(p.context, listSql, result)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (p PaginationParam) Pagination(result any) (count int64, err error) {
	p.resultDst = result
	isShardedTable := p.GetTable().isShardedTable()
	if isShardedTable {
		shardedTablePaginationBuilder := NewShardedTablePaginationBuilder(p)
		count, err = shardedTablePaginationBuilder.Pagination(result)
		if err != nil {
			return 0, err
		}
		return count, nil
	}
	totalSql, listSql, err := p.ToSQL()
	if err != nil {
		return 0, err
	}

	index, size := p._Fields.Pagination()
	if index == 0 && size == 0 {
		err = ErrPaginationSizeRequired
		return 0, err
	}

	return p.paginationHandler(totalSql, listSql, result)
}

type SetPolicy string

const (
	SetPolicy_only_Insert       SetPolicy = "onlyInsert"      //只新增说明使用最早数据(场景: 1. 初始化数据表)
	SetPolicy_only_Update       SetPolicy = "onlyUpdate"      //只更新说明不存在时不处理
	SetPolicy_Insert_or_Update  SetPolicy = ""                //不存在新增,存在更新，使用最新数据覆盖
	SetPolicy_Delete_and_insert SetPolicy = "deleteAndInsert" //先删除再新增

)

var setPolicy_need_insert_sql = []SetPolicy{SetPolicy_only_Insert, SetPolicy_Insert_or_Update, SetPolicy_Delete_and_insert}
var setPolicy_need_update_sql = []SetPolicy{SetPolicy_only_Update, SetPolicy_Insert_or_Update}
var setPolicy_need_delete_sql = []SetPolicy{SetPolicy_Delete_and_insert}
var setPolicy_no_exits_sql = []SetPolicy{SetPolicy_only_Insert} // 指明只新增，不需要查询是否存在，直接新增

type SetParam struct {
	setPolicy             SetPolicy // 更新策略,默认根据主键判断是否需要更新
	_triggerInsertedEvent EventInsertTrigger
	_triggerUpdatedEvent  EventUpdateTrigger
	_triggerDeletedEvent  EventDeletedTrigger
	SQLParam[SetParam]
}

func NewSetBuilder(tableConfig TableConfig) *SetParam {
	p := &SetParam{}
	p.SQLParam = NewSQLParam(p, tableConfig)
	return p
}

func (p *SetParam) WithPolicy(policy SetPolicy) *SetParam {
	p.setPolicy = policy
	return p
}

func (p *SetParam) WithTriggerEvent(triggerInsertdEvent EventInsertTrigger, triggerUpdateEvent EventUpdateTrigger) *SetParam {
	p._triggerInsertedEvent = triggerInsertdEvent
	p._triggerUpdatedEvent = triggerUpdateEvent
	return p
}

func (p *SetParam) getEventHandler() (triggerInsertdEvent EventInsertTrigger, triggerUpdateEvent EventUpdateTrigger, triggerDeleteEvent EventDeletedTrigger) {
	triggerInsertdEvent = p._triggerInsertedEvent
	if triggerInsertdEvent == nil {
		triggerInsertdEvent = func(lastInsertId uint64, rowsAffected int64) (err error) { return nil }
	}
	triggerUpdateEvent = p._triggerUpdatedEvent
	if triggerUpdateEvent == nil {
		triggerUpdateEvent = func(rowsAffected int64) (err error) { return nil }
	}
	triggerDeleteEvent = p._triggerDeletedEvent
	if triggerDeleteEvent == nil {
		triggerDeleteEvent = func(rowsAffected int64) (err error) { return nil }
	}

	return triggerInsertdEvent, triggerUpdateEvent, triggerDeleteEvent
}

type CustomFnSetParam = CustomFn[SetParam]
type CustomFnSetParams = CustomFns[SetParam]

func (p *SetParam) ApplyCustomFn(customFns ...CustomFnSetParam) *SetParam {
	p = CustomFns[SetParam](customFns).Apply(p)
	return p
}

// 2026-06-26 14:57 这个修改不兼容，最开始 当下的ToSQLV0 是历史的 ToSQL, 为了和其它保持一致,让渡了ToSQL函数签名,ToSQLV0 保持原有函数签名
func (p SetParam) ToSQLV0() (existsSql string, insertSql string, updateSql string, err error) {
	existsSql, insertSql, updateSql, _, err = p.ToSQL()
	if err != nil {
		return "", "", "", err
	}
	return existsSql, insertSql, updateSql, nil
}

func (p SetParam) ToSQL() (existsSql string, insertSql string, updateSql string, deleteSql string, err error) {
	table := p.GetTable()
	if !slices.Contains(setPolicy_no_exits_sql, p.setPolicy) { // 如果指定只新增则不需要查询是否存在
		existsSql, err = NewExistsBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).ToSQL() // 有些根据场景设置 如枚举值 ""，所有需要复制
		if errors.Is(err, ErrEmptyWhere) {                                                                                   //查询是否存在，没有where条件，说明需要直接insert 比如 id=0 时，此时不存在，直接新增即可
			p.WithPolicy(SetPolicy_only_Insert) // 设置为只新增，避免其他报错
			err = nil                           // 注意，这种情况会输出insertsql，existsSql 为空，所以只需existsSql是，需要判空
		}
	}

	if err != nil {
		return "", "", "", "", err
	}
	if slices.Contains(setPolicy_need_insert_sql, p.setPolicy) {
		insertSql, err = NewInsertBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).ToSQL()
		if err != nil {
			return "", "", "", "", err
		}
	}

	if slices.Contains(setPolicy_need_update_sql, p.setPolicy) { // 不加条件判断 当 setPolicy 为 SetPolicy_only_Insert 可能报错，比如：只有唯一键一列，更新时屏蔽唯一键更新，此时更新字段就为空，会报错，所以增加if 判断
		updateSql, err = NewUpdateBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).ToSQL()
		if err != nil {
			return "", "", "", "", err
		}
	}
	if slices.Contains(setPolicy_need_delete_sql, p.setPolicy) {
		deleteSql, err = NewDeleteBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).ToSQL()
		if err != nil {
			return "", "", "", "", err
		}
	}
	return existsSql, insertSql, updateSql, deleteSql, nil
}

func (p SetParam) Set() (isNotExits bool, lastInsertId uint64, rowsAffected int64, err error) {
	// 因为自带 exists 查询，所以不需要校验唯一索引了，否则 存在 table.CheckUniqueIndex 就会报错，达不到update 效果
	// table := p.GetTable()
	// err = table.CheckUniqueIndex(p._Fields...)
	// if err != nil {
	// 	return false, 0, 0, err
	// }
	// 2025-08-21 如果whereData 为空则只能执行新增（比如主键id为0,使得数据必然不存在，可以略过查询是否存在）
	existsSql, insertSql, updateSql, deleteSql, err := p.ToSQL()
	if err != nil {
		return false, 0, 0, err
	}

	existsHandler := WithSingleflightDoOnce(p.GetHandlerWithInitTable().OriginalHandler()).Exists // 屏蔽缓存中间件，同时防止单实例并发问题
	triggerInsertdEvent, triggerUpdateEvent, triggerDeletedEvent := p.getEventHandler()
	withInsertEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = triggerInsertdEvent(event.LastInsertId, event.RowsAffected)
		if err != nil {
			p.Log(insertSql, err)
		}
	})
	withUpdateEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = triggerUpdateEvent(event.RowsAffected)
		if err != nil {
			p.Log(updateSql, err)
		}
	})
	withDeletedEventHandler := WithTriggerAsyncEvent(p.GetHandlerWithInitTable(), func(event *Event) {
		err = triggerDeletedEvent(event.RowsAffected)
		if err != nil {
			p.Log(deleteSql, err)
		}
	})

	exists := false
	if existsSql != "" { //where 条件为空，existsSql 返回空，但是  insertSql 不为空，需要执行，所以这里需要判空
		exists, err = existsHandler(existsSql)
		if err != nil {
			return false, 0, 0, err
		}
	}
	isNotExits = !exists
	switch p.setPolicy {
	case SetPolicy_only_Insert: // 只新增说明使用最早数据
		if !exists {
			lastInsertId, rowsAffected, err = withInsertEventHandler.InsertWithLastId(insertSql)
			return isNotExits, lastInsertId, rowsAffected, err
		}
	case SetPolicy_only_Update: // 只更新说明不存在时不处理
		if exists {
			rowsAffected, err = withUpdateEventHandler.ExecWithRowsAffected(updateSql)
			return isNotExits, lastInsertId, rowsAffected, err
		}
	case SetPolicy_Delete_and_insert:
		if exists {
			rowsAffected, err = withDeletedEventHandler.ExecWithRowsAffected(deleteSql)
			if err != nil {
				return isNotExits, lastInsertId, rowsAffected, err
			}
		}
		lastInsertId, rowsAffected, err = withInsertEventHandler.InsertWithLastId(insertSql)
		return isNotExits, lastInsertId, rowsAffected, err
	default: // 默认执行 SetPolicy_Insert_or_Update 策略
		if exists {
			rowsAffected, err = withUpdateEventHandler.ExecWithRowsAffected(updateSql)
		} else {
			lastInsertId, rowsAffected, err = withInsertEventHandler.InsertWithLastId(insertSql)
		}
	}
	return isNotExits, lastInsertId, rowsAffected, err
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

type SQLParam[T any] struct {
	self            *T
	_Table          TableConfig
	_Fields         Fields
	_log            LogI
	context         context.Context
	resultDst       any
	customFieldsFns CustomFieldsFns // 定制化字段处理函数，可用于局部封装字段处理逻辑，比如通用模型中，用于设置查询字段的别名
}

func NewSQLParam[T any](self *T, table TableConfig) SQLParam[T] {
	return SQLParam[T]{
		self:    self,
		_Table:  table,
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p *SQLParam[T]) WithContext(ctx context.Context) *T {
	p.context = ctx
	return p.self
}

func (p *SQLParam[T]) WithCustomFieldsFn(fns ...CustomFieldsFn) *T {
	p.customFieldsFns = fns
	return p.self
}

func (p *SQLParam[T]) WithHandler(handler Handler) *T {
	if IsNil(handler) {
		return p.self
	}
	p._Table = p._Table.WithHandler(handler)
	return p.self
}
func (p *SQLParam[T]) WithResultDest(dst any) *T {
	p.resultDst = dst
	return p.self
}

func (p *SQLParam[T]) GetHandlerWithInitTable() (handler Handler) {
	return p._Table.GetHandlerWithInitTable()
}

func (p *SQLParam[T]) GetGoquDialect() goqu.DialectWrapper {
	dialect := string(Driver_mysql)
	if p._Table._handler != nil {
		dialect = p._Table.GetHandler().GetDialector()
	}
	return goqu.Dialect(dialect)
}

func (p *SQLParam[T]) WithHandlerMiddleware(middlewares ...HandlerMiddleware) *T {
	p._Table = p._Table.WithHandler(ChainHandler(p.GetHandlerWithInitTable(), middlewares...))
	return p.self
}

func (p *SQLParam[T]) AppendFields(fs ...*Field) *T {
	p._Fields.Append(fs...)
	return p.self
}

func (p *SQLParam[T]) Fields() Fields {
	return p._Fields
}

func (p *SQLParam[T]) GetTable() TableConfig {
	return p._Table
}

func (p *SQLParam[T]) SetLog(log LogI) *T {
	p._log = log
	return p.self
}
func (p *SQLParam[T]) getSelectColumns(fs Fields) (selectColumns []any) {
	selectColumns = fs.Select()
	if len(selectColumns) > 0 {
		return selectColumns
	}
	//这种转换过于隐蔽，容易引起bug，建议需要转换时，手动明确设置Field.SetSelectColumn()

	// if p.resultDst != nil {
	// 	resultFs, ok := TryGetFields(p.resultDst)
	// 	if ok {
	// 		selectColumns = resultFs.MakeDBColumnWithAlias(table.Columns)
	// 		return selectColumns
	// 	}
	// }

	return selectColumns
}
func (p *SQLParam[T]) Log(sql string, args ...any) {
	if p._log != nil {
		p._log.Log(sql, args...)
	}
}

type IdentityI interface {
	GetIdentity() string
}

// SplitAddUpdateDelete 拆分新增、更新、删除数据，方便批量处理

func SplitAddUpdateDelete[T IdentityI](newSet []T, oldSet []T) (addSet []T, updateSet []T, deleteSet []T) {
	oldTable := memorytable.NewTable(oldSet...)
	newTable := memorytable.NewTable(newSet...)

	//取交集，即需要更新的行为
	intersect, _ := newTable.Intersection(oldTable, func(row T) string {
		return row.GetIdentity()
	})
	updateTaskBehaviors, _ := intersect.ToSliceWithEmpty()

	//取差集，在新集合内，不在旧集合内的数据即为需要新增的数据
	addTaskBehaviors, _ := newTable.Diff(oldTable, func(row T) string {
		return row.GetIdentity()
	}).ToSliceWithEmpty()

	// 取差集，在旧集合内，不在新集合内的数据即为需要删除的数据
	deleteTaskBehaviors, _ := newTable.Diff(oldTable, func(row T) string {
		return row.GetIdentity()
	}).ToSliceWithEmpty()
	return addTaskBehaviors, updateTaskBehaviors, deleteTaskBehaviors
}
