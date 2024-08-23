package sqlbuilder

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type Builder struct {
	handler Handler
	table   string
}

func NewGormBuilder(table string, getDB func() *gorm.DB) *Builder {
	handler := NewGormHandler(getDB)
	return &Builder{handler: handler, table: table}
}

func NewBuilder(table string, handler Handler) *Builder {
	return &Builder{handler: handler, table: table}
}

func (b *Builder) Count(fields ...*Field) (count int64, err error) {
	return NewTotalBuilder(b.table).AppendFields(fields...).Count(b.handler.Count)
}

func (b *Builder) List(result any, fields ...*Field) (err error) {
	return NewListBuilder(b.table).AppendFields(fields...).Query(result, b.handler.Query)
}

func (b *Builder) Pagination(result any, fields ...*Field) (count int64, err error) {
	return NewPaginationBuilder(b.table).AppendFields(fields...).Pagination(result, b.handler.Pagination)
}

func (b *Builder) First(result any, fields ...*Field) (exists bool, err error) {
	return NewFirstBuilder(b.table).AppendFields(fields...).First(result, b.handler.First)
}
func (b *Builder) Insert(fields ...*Field) (err error) {
	return NewInsertBuilder(b.table).AppendFields(fields...).Exec(b.handler.Exec)
}
func (b *Builder) Update(fields ...*Field) (err error) {
	return NewUpdateBuilder(b.table).AppendFields(fields...).Exec(b.handler.Exec)
}
func (b *Builder) Delete(fields ...*Field) (err error) {
	return NewDeleteBuilder(b.table).AppendFields(fields...).Exec(b.handler.Exec)
}

func (b *Builder) Exists(fields ...*Field) (exists bool, err error) {
	return NewExistsBuilder(b.table).AppendFields(fields...).Exists(b.handler.Query)
}

type Driver string

func (d Driver) String() string {
	return string(d)
}

func (d Driver) IsSame(target Driver) bool {
	return strings.EqualFold(d.String(), target.String())
}

type Expressions []goqu.Expression

func (exs Expressions) IsEmpty() bool {
	return len(exs) == 0
}

var ERROR_EMPTY_WHERE = errors.New("error  empty where")

const (
	Driver_mysql   Driver = "mysql"
	Driver_sqlite3 Driver = "sqlite3"
)

type EscapeString func(value string) string

var MysqlEscapeString EscapeString = func(value string) string {
	var sb strings.Builder
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch c {
		case '\\', 0, '\n', '\r', '\'', '"':
			sb.WriteByte('\\')
			sb.WriteByte(c)
		case '\032':
			sb.WriteByte('\\')
			sb.WriteByte('Z')
		default:
			sb.WriteByte(c)
		}
	}
	return sb.String()
}
var SQLite3EscapeString EscapeString = MysqlEscapeString // 此处暂时使用mysql的

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

type Scene string // 迁移场景

func (s Scene) Is(target Scene) bool {
	return strings.EqualFold(string(s), string(target))
}

const (
	SCENE_SQL_INSERT   Scene = "insert"
	SCENE_SQL_UPDATE   Scene = "update"
	SCENE_SQL_DELETE   Scene = "delete"
	SCENE_SQL_SELECT   Scene = "select"
	SCENE_SQL_VIEW     Scene = "view"
	SCENE_SQL_INCREASE Scene = "increse" // 字段递增
	SCENE_SQL_DECREASE Scene = "decrese" // 字段递减
)

type TableI interface {
	Table() (table string)
}
type TableFn func() (table string)

func (fn TableFn) Table() (table string) {
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

// InsertParam 供子类复用,修改数据
type InsertParam struct {
	_TableI TableI
	_Fields Fields
	_log    LogI
}

func (p *InsertParam) SetLog(log LogI) InsertParam {
	p._log = log
	return *p
}

func NewInsertBuilder(tableName string) InsertParam {
	return InsertParam{
		_TableI: TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p InsertParam) AppendFields(fields ...*Field) InsertParam {
	p._Fields.Append(fields...)
	return p
}

func (p InsertParam) _Data() (data any, err error) {
	return p._Fields.Data()
}

func (p InsertParam) ToSQL() (sql string, err error) {
	p._Fields.SetSceneIfEmpty(SCENE_SQL_INSERT)
	rowData, err := p._Data()
	if err != nil {
		return "", err
	}
	if IsNil(rowData) {
		err = errors.New("InsertParam.Data() return nil data")
		return "", err
	}
	if p._TableI == nil {
		err = errors.Errorf("InsertParam._Table required")
		return "", err
	}
	table := p._TableI.Table()
	ds := Dialect.DialectWrapper().Insert(table).Rows(rowData)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

func (p InsertParam) Exec(execHandler ExecHandler) (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	err = execHandler(sql)
	return err
}

type InsertParams struct {
	rowFields []Fields
	_TableI   TableI
	_log      LogI
}

func NewInsertsBuilder(tableName string) InsertParams {
	return InsertParams{
		_TableI:   TableFn(func() string { return tableName }),
		rowFields: make([]Fields, 0),
		_log:      DefaultLog,
	}
}

func (p *InsertParams) SetLog(log LogI) InsertParams {
	p._log = log
	return *p
}
func (p InsertParams) AppendFields(fields ...Fields) InsertParams {
	if p.rowFields == nil {
		p.rowFields = make([]Fields, 0)
	}
	p.rowFields = append(p.rowFields, fields...)
	return p
}

func (is InsertParams) ToSQL() (sql string, err error) {
	data := make([]any, 0)
	for _, fields := range is.rowFields {
		rowData, err := fields.Data()
		if err != nil {
			return "", err
		}
		if IsNil(rowData) {
			continue
		}
		data = append(data, rowData)
	}
	ds := Dialect.DialectWrapper().Insert(is._TableI.Table()).Rows(data...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if is._log != nil {
		is._log.Log(sql)
	}
	return sql, nil
}

type DeleteParam struct {
	_TableI TableI
	_Fields Fields
	_log    LogI
}

func (p *DeleteParam) SetLog(log LogI) DeleteParam {
	p._log = log
	return *p
}

func NewDeleteBuilder(tableName string) DeleteParam {
	return DeleteParam{
		_TableI: TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p DeleteParam) AppendFields(fields ...*Field) DeleteParam {
	p._Fields.Append(fields...)
	return p
}

func (p DeleteParam) _Data() (data any, err error) {
	return p._Fields.Data()
}

func (p DeleteParam) _Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p DeleteParam) ToSQL() (sql string, err error) {
	p._Fields.SetSceneIfEmpty(SCENE_SQL_DELETE)
	_, ok := p._Fields.GetByFieldName(Field_name_deletedAt)
	if !ok {
		err = errors.Errorf("not found deleted column by fieldName:%s", Field_name_deletedAt)
		return "", err
	}
	data, err := p._Data()
	if err != nil {
		return "", err
	}

	where, err := p._Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.DialectWrapper().Update(p._TableI.Table()).Set(data).Where(where...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}
func (p DeleteParam) Exec(execHandler ExecHandler) (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	err = execHandler(sql)
	return err
}

type UpdateParam struct {
	_TableI TableI
	_Fields Fields
	_log    LogI
}

func (p *UpdateParam) SetLog(log LogI) UpdateParam {
	p._log = log
	return *p
}

func NewUpdateBuilder(tableName string) UpdateParam {
	return UpdateParam{
		_TableI: TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p UpdateParam) AppendFields(fields ...*Field) UpdateParam {
	p._Fields.Append(fields...)
	return p
}

func (p UpdateParam) _Data() (data any, err error) {
	return p._Fields.Data()
}

func (p UpdateParam) _Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p UpdateParam) ToSQL() (sql string, err error) {
	p._Fields.SetSceneIfEmpty(SCENE_SQL_UPDATE)
	data, err := p._Data()
	if err != nil {
		return "", err
	}

	where, err := p._Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.DialectWrapper().Update(p._TableI.Table()).Set(data).Where(where...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

func (p UpdateParam) Exec(execHandler ExecHandler) (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	err = execHandler(sql)
	return err
}

// type FirstParamI interface {
// 	TableI
// 	_Select
// }

type FirstParam struct {
	_Table  TableI
	_Fields Fields
	_log    LogI
}

func (p *FirstParam) SetLog(log LogI) FirstParam {
	p._log = log
	return *p
}

func (p FirstParam) AppendFields(fields ...*Field) FirstParam {
	p._Fields.Append(fields...)
	return p
}

func NewFirstBuilder(tableName string, columns ...any) FirstParam {
	return FirstParam{
		_Table:  TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p FirstParam) _Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p FirstParam) _Order() (orderedExpression []exp.OrderedExpression) {
	return p._Fields.Order()
}

func (p FirstParam) ToSQL() (sql string, err error) {
	p._Fields.SetSceneIfEmpty(SCENE_SQL_SELECT)
	where, err := p._Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.DialectWrapper().Select(p._Fields.Select()...).
		From(p._Table.Table()).
		Where(where...).
		Order(p._Order()...).
		Limit(1)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

func (p FirstParam) First(result any, firstHandler FirstHandler) (exists bool, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return false, err
	}
	return firstHandler(sql, result)
}

type ListParam struct {
	_Table  TableI
	_Fields Fields
	_log    LogI
}

func (p *ListParam) SetLog(log LogI) ListParam {
	p._log = log
	return *p
}

func (p ListParam) AppendFields(fields ...*Field) ListParam {
	p._Fields.Append(fields...)
	return p
}

func NewListBuilder(tableName string) ListParam {
	return ListParam{
		_Table: TableFn(func() string { return tableName }),
		_log:   DefaultLog,
	}
}

func (p ListParam) _Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}
func (p ListParam) _Order() (orderedExpression []exp.OrderedExpression) {
	return p._Fields.Order()
}

func (p ListParam) ToSQL() (sql string, err error) {
	p._Fields.SetSceneIfEmpty(SCENE_SQL_SELECT)
	where, err := p._Where()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := p._Fields.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.DialectWrapper().Select(p._Fields.Select()...).
		From(p._Table.Table()).
		Where(where...).
		Order(p._Order()...)
	if pageSize > 0 {
		ds = ds.Offset(uint(ofsset)).Limit(uint(pageSize))
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

func (p ListParam) Query(result any, queryHandler QueryHandler) (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	return queryHandler(sql, result)
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
	_Table  TableI
	_Fields Fields
	_log    LogI
}

func (p ExistsParam) AppendFields(fields ...*Field) ExistsParam {
	p._Fields.Append(fields...)
	return p
}
func (p *ExistsParam) SetLog(log LogI) ExistsParam {
	p._log = log
	return *p
}

func NewExistsBuilder(tableName string) ExistsParam {
	return ExistsParam{
		_Table: TableFn(func() string { return tableName }),
		_log:   DefaultLog,
	}
}

func (p ExistsParam) _Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p ExistsParam) ToSQL() (sql string, err error) {
	p._Fields.SetSceneIfEmpty(SCENE_SQL_SELECT)
	where, err := p._Where()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := p._Fields.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.DialectWrapper().Select(goqu.L("1").As("count")).
		From(p._Table.Table()).
		Where(where...).
		Limit(1)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return sql, nil
}

func (p ExistsParam) Exists(queryHandler QueryHandler) (exists bool, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return false, err
	}
	result := make([]any, 0)
	err = queryHandler(sql, &result)
	if err != nil {
		return false, err
	}
	if len(result) > 0 {
		return true, nil
	}
	return false, nil
}

type TotalParam struct {
	_Table  TableI
	_Fields Fields
	_log    LogI
}

func NewTotalBuilder(tableName string) TotalParam {
	return TotalParam{
		_Table: TableFn(func() string { return tableName }),
		_log:   DefaultLog,
	}
}
func (p *TotalParam) SetLog(log LogI) TotalParam {
	p._log = log
	return *p
}

func (p TotalParam) AppendFields(fields ...*Field) TotalParam {
	p._Fields.Append(fields...)
	return p
}

func (p TotalParam) Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p TotalParam) ToSQL() (sql string, err error) {
	p._Fields.SetSceneIfEmpty(SCENE_SQL_SELECT)
	where, err := p.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.DialectWrapper().From(p._Table.Table()).Where(where...).Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

func (p TotalParam) Count(countHandler CountHandler) (total int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return -1, err
	}
	if p._log != nil {
		p._log.Log(sql)
	}
	return countHandler(sql)
}

type PaginationParam struct {
	_Table  TableI
	_Fields Fields
}

func (p PaginationParam) AppendFields(fields ...*Field) PaginationParam {
	p._Fields.Append(fields...)
	return p
}

func NewPaginationBuilder(tableName string) PaginationParam {
	return PaginationParam{
		_Table: TableFn(func() string { return tableName }),
	}
}

func (p PaginationParam) ToSQL() (totalSql string, listSql string, err error) {
	table := p._Table.Table()
	totalSql, err = NewTotalBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", err
	}
	listSql, err = NewTotalBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", err
	}
	return totalSql, listSql, nil
}

func (p PaginationParam) Pagination(result any, paginationHandler PaginationHandler) (count int64, err error) {
	table := p._Table.Table()
	totalSql, err := NewTotalBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return 0, err
	}
	listSql, err := NewListBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return 0, err
	}
	return paginationHandler(totalSql, listSql, result)
}

type SetParam struct {
	_Table  TableI
	_Fields Fields
}

func (p SetParam) AppendFields(fields ...*Field) SetParam {
	p._Fields.Append(fields...)
	return p
}

func NewSetBuilder(tableName string) SetParam {
	return SetParam{
		_Table: TableFn(func() string { return tableName }),
	}
}

// ToSQL 一次生成 查询、新增、修改 sql,若查询后记录存在,并且需要根据数据库记录值修改数据,则可以重新赋值后生成sql
func (p SetParam) ToSQL() (existsSql string, insertSql string, updateSql string, err error) {
	table := p._Table.Table()
	existsSql, err = NewExistsBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", "", err
	}
	insertSql, err = NewInsertBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", "", err
	}
	updateSql, err = NewUpdateBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", "", err
	}
	return existsSql, insertSql, updateSql, nil
}

func (p SetParam) Set(queryHandler QueryHandler, execHandler ExecHandler) error {
	table := p._Table.Table()
	exists, err := NewExistsBuilder(table).AppendFields(p._Fields...).Exists(queryHandler)
	if err != nil {
		return err
	}
	if exists {
		err = NewUpdateBuilder(table).AppendFields(p._Fields...).Exec(execHandler)
	} else {
		err = NewInsertBuilder(table).AppendFields(p._Fields...).Exec(execHandler)
	}
	return err
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
