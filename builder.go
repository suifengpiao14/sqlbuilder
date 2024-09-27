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
	return NewTotalBuilder(b.table).WithHandler(b.handler.Count).AppendFields(fields...).Count()
}

func (b *Builder) List(result any, fields ...*Field) (err error) {
	return NewListBuilder(b.table).WithHandler(b.handler.Query).AppendFields(fields...).Query(result)
}

func (b *Builder) Pagination(result any, fields ...*Field) (count int64, err error) {
	return NewPaginationBuilder(b.table).WithHandler(b.handler.Pagination).AppendFields(fields...).Pagination(result)
}

func (b *Builder) First(result any, fields ...*Field) (exists bool, err error) {
	return NewFirstBuilder(b.table).WithHandler(b.handler.First).AppendFields(fields...).First(result)
}
func (b *Builder) Insert(fields ...*Field) (err error) {
	return NewInsertBuilder(b.table).WithHandler(b.handler.Exec, nil).AppendFields(fields...).Exec()
}
func (b *Builder) InsertWithLastInsertId(fields ...*Field) (lastInsertId uint64, rowsAffected int64, err error) {
	return NewInsertBuilder(b.table).WithHandler(nil, b.handler.InsertWithLastIdHandler).AppendFields(fields...).InsertWithLastId()
}
func (b *Builder) Update(fields ...*Field) (err error) {
	return NewUpdateBuilder(b.table).WithHandler(b.handler.ExecWithRowsAffected).AppendFields(fields...).Exec()
}
func (b *Builder) UpdateWithRowsAffected(fields ...*Field) (rowsAffected int64, err error) {
	return NewUpdateBuilder(b.table).WithHandler(b.handler.ExecWithRowsAffected).AppendFields(fields...).ExecWithRowsAffected()
}
func (b *Builder) Delete(fields ...*Field) (err error) {
	return NewDeleteBuilder(b.table).WithHandler(b.handler.ExecWithRowsAffected).AppendFields(fields...).Exec()
}
func (b *Builder) DeleteWithRowsAffected(fields ...*Field) (rowsAffected int64, err error) {
	return NewDeleteBuilder(b.table).WithHandler(b.handler.ExecWithRowsAffected).AppendFields(fields...).ExecWithRowsAffected()
}

func (b *Builder) Exists(fields ...*Field) (exists bool, err error) {
	return NewExistsBuilder(b.table).WithHandler(b.handler.Query).AppendFields(fields...).Exists()
}
func (b *Builder) Set(fields ...*Field) (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	return NewSetBuilder(b.table).WithHandler(b.handler.Query, b.handler.InsertWithLastIdHandler, b.handler.ExecWithRowsAffected).AppendFields(fields...).Set()
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
	SCENE_SQL_INIT     Scene = "init"
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
	_TableI                 TableI
	_Fields                 Fields
	_log                    LogI
	execHandler             ExecHandler
	insertWithLastIdHandler InsertWithLastIdHandler
}

func (p *InsertParam) SetLog(log LogI) InsertParam {
	p._log = log
	return *p
}
func (p *InsertParam) WithHandler(execHandler ExecHandler, insertWithLastIdHandler InsertWithLastIdHandler) *InsertParam {
	p.execHandler = execHandler
	p.insertWithLastIdHandler = insertWithLastIdHandler
	return p
}

func NewInsertBuilder(tableName string) *InsertParam {
	return &InsertParam{
		_TableI: TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p *InsertParam) AppendFields(fields ...*Field) *InsertParam {
	p._Fields.Append(fields...)
	return p
}

func (p InsertParam) ToSQL() (sql string, err error) {
	fs := p._Fields.Copy() // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	fs.SetSceneIfEmpty(SCENE_SQL_INSERT)
	rowData, err := fs.Data()
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

func (p InsertParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	err = p.execHandler(sql)
	return err
}
func (p InsertParam) InsertWithLastId() (lastInsertId uint64, rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return 0, 0, err
	}
	return p.insertWithLastIdHandler(sql)
}

type BatchInsertParam struct {
	rowFields               []Fields
	_TableI                 TableI
	_log                    LogI
	execHandler             ExecHandler
	insertWithLastIdHandler InsertWithLastIdHandler
}

func NewBatchInsertBuilder(tableName string) *BatchInsertParam {
	return &BatchInsertParam{
		_TableI:   TableFn(func() string { return tableName }),
		rowFields: make([]Fields, 0),
		_log:      DefaultLog,
	}
}

func (p *BatchInsertParam) SetLog(log LogI) *BatchInsertParam {
	p._log = log
	return p
}

func (p *BatchInsertParam) WithHandler(execHandler ExecHandler, insertWithLastIdHandler InsertWithLastIdHandler) *BatchInsertParam {
	p.execHandler = execHandler
	p.insertWithLastIdHandler = insertWithLastIdHandler
	return p
}

func (p BatchInsertParam) AppendFields(fields ...Fields) BatchInsertParam {
	if p.rowFields == nil {
		p.rowFields = make([]Fields, 0)
	}
	p.rowFields = append(p.rowFields, fields...)
	return p
}

var ERROR_BATCH_INSERT_DATA_IS_NIL = errors.New("batch insert err: data is nil")

func (is BatchInsertParam) ToSQL() (sql string, err error) {
	data := make([]any, 0)
	if len(is.rowFields) == 0 {
		return "", ERROR_BATCH_INSERT_DATA_IS_NIL
	}
	for _, fields := range is.rowFields {
		fs := fields.Copy() // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
		fs.SetSceneIfEmpty(SCENE_SQL_INSERT)
		rowData, err := fs.Data()
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
func (p BatchInsertParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	err = p.execHandler(sql)
	return err
}
func (p BatchInsertParam) InsertWithLastId() (lastInsertId uint64, rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return 0, 0, err
	}
	return p.insertWithLastIdHandler(sql)
}

type DeleteParam struct {
	_TableI                     TableI
	_Fields                     Fields
	_log                        LogI
	execWithRowsAffectedHandler ExecWithRowsAffectedHandler
}

func (p *DeleteParam) SetLog(log LogI) DeleteParam {
	p._log = log
	return *p
}
func (p *DeleteParam) WithHandler(execWithRowsAffectedHandler ExecWithRowsAffectedHandler) *DeleteParam {
	p.execWithRowsAffectedHandler = execWithRowsAffectedHandler
	return p
}

func NewDeleteBuilder(tableName string) *DeleteParam {
	return &DeleteParam{
		_TableI: TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p *DeleteParam) AppendFields(fields ...*Field) *DeleteParam {
	p._Fields.Append(fields...)
	return p
}

func (p DeleteParam) ToSQL() (sql string, err error) {
	fs := p._Fields.Copy() // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	fs.SetSceneIfEmpty(SCENE_SQL_DELETE)
	_, ok := fs.GetByFieldName(Field_name_deletedAt)
	if !ok {
		err = errors.Errorf("not found deleted column by fieldName:%s", Field_name_deletedAt)
		return "", err
	}
	data, err := fs.Data()
	if err != nil {
		return "", err
	}

	where, err := fs.Where()
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
func (p DeleteParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	_, err = p.execWithRowsAffectedHandler(sql)
	return err
}
func (p DeleteParam) ExecWithRowsAffected() (rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return rowsAffected, err
	}
	rowsAffected, err = p.execWithRowsAffectedHandler(sql)
	return rowsAffected, err
}

type UpdateParam struct {
	_TableI                     TableI
	_Fields                     Fields
	_log                        LogI
	execWithRowsAffectedHandler ExecWithRowsAffectedHandler
}

func (p *UpdateParam) SetLog(log LogI) UpdateParam {
	p._log = log
	return *p
}
func (p *UpdateParam) WithHandler(execWithRowsAffectedHandler ExecWithRowsAffectedHandler) *UpdateParam {
	p.execWithRowsAffectedHandler = execWithRowsAffectedHandler
	return p
}

func NewUpdateBuilder(tableName string) *UpdateParam {
	return &UpdateParam{
		_TableI: TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p *UpdateParam) AppendFields(fields ...*Field) *UpdateParam {
	p._Fields.Append(fields...)
	return p
}

func (p UpdateParam) ToSQL() (sql string, err error) {
	fs := p._Fields.Copy() // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	fs.SetSceneIfEmpty(SCENE_SQL_UPDATE)
	data, err := fs.Data()
	if err != nil {
		return "", err
	}

	where, err := fs.Where()
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

func (p UpdateParam) Exec() (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	_, err = p.execWithRowsAffectedHandler(sql)
	return err
}
func (p UpdateParam) ExecWithRowsAffected() (rowsAffected int64, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return 0, err
	}
	rowsAffected, err = p.execWithRowsAffectedHandler(sql)
	return rowsAffected, err
}

type FirstParam struct {
	_Table       TableI
	_Fields      Fields
	_log         LogI
	firstHandler FirstHandler
}

func (p *FirstParam) SetLog(log LogI) *FirstParam {
	p._log = log
	return p
}

func (p FirstParam) AppendFields(fields ...*Field) FirstParam {
	p._Fields.Append(fields...)
	return p
}

func NewFirstBuilder(tableName string) *FirstParam {
	return &FirstParam{
		_Table:  TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
		_log:    DefaultLog,
	}
}

func (p *FirstParam) WithHandler(firstHandler FirstHandler) *FirstParam {
	p.firstHandler = firstHandler
	return p
}

func (p FirstParam) ToSQL() (sql string, err error) {
	fs := p._Fields.Copy() // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	fs.SetSceneIfEmpty(SCENE_SQL_SELECT)
	where, err := fs.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.DialectWrapper().Select(fs.Select()...).
		From(p._Table.Table()).
		Where(where...).
		Order(fs.Order()...).
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

func (p FirstParam) First(result any) (exists bool, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return false, err
	}
	return p.firstHandler(sql, result)
}

type ListParam struct {
	_Table       TableI
	_Fields      Fields
	_log         LogI
	queryHandler QueryHandler
}

func (p *ListParam) SetLog(log LogI) ListParam {
	p._log = log
	return *p
}
func (p *ListParam) WithHandler(queryHandler QueryHandler) *ListParam {
	p.queryHandler = queryHandler
	return p
}

func (p ListParam) AppendFields(fields ...*Field) ListParam {
	p._Fields.Append(fields...)
	return p
}

func NewListBuilder(tableName string) *ListParam {
	return &ListParam{
		_Table: TableFn(func() string { return tableName }),
		_log:   DefaultLog,
	}
}

func (p ListParam) ToSQL() (sql string, err error) {
	fs := p._Fields.Copy() // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	fs.SetSceneIfEmpty(SCENE_SQL_SELECT)
	where, err := fs.Where()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := fs.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.DialectWrapper().Select(fs.Select()...).
		From(p._Table.Table()).
		Where(where...).
		Order(fs.Order()...)
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

func (p ListParam) Query(result any) (err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return err
	}
	return p.queryHandler(sql, result)
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
	_Table       TableI
	_Fields      Fields
	_log         LogI
	queryHandler QueryHandler
}

func (p *ExistsParam) AppendFields(fields ...*Field) *ExistsParam {
	p._Fields.Append(fields...)
	return p
}
func (p *ExistsParam) SetLog(log LogI) ExistsParam {
	p._log = log
	return *p
}

func (p *ExistsParam) WithHandler(queryHandler QueryHandler) *ExistsParam {
	p.queryHandler = queryHandler
	return p
}

func NewExistsBuilder(tableName string) *ExistsParam {
	return &ExistsParam{
		_Table: TableFn(func() string { return tableName }),
		_log:   DefaultLog,
	}
}

func (p ExistsParam) ToSQL() (sql string, err error) {
	fs := p._Fields.Copy() // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	fs.SetSceneIfEmpty(SCENE_SQL_SELECT)
	where, err := fs.Where()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := fs.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.DialectWrapper().Select(goqu.L("1").As("exists")).
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

func (p ExistsParam) Exists() (exists bool, err error) {
	sql, err := p.ToSQL()
	if err != nil {
		return false, err
	}
	result := make([]any, 0)
	err = p.queryHandler(sql, &result)
	if err != nil {
		return false, err
	}
	if len(result) > 0 {
		return true, nil
	}
	return false, nil
}

type TotalParam struct {
	_Table       TableI
	_Fields      Fields
	_log         LogI
	countHandler CountHandler
}

func NewTotalBuilder(tableName string) *TotalParam {
	return &TotalParam{
		_Table: TableFn(func() string { return tableName }),
		_log:   DefaultLog,
	}
}
func (p *TotalParam) SetLog(log LogI) *TotalParam {
	p._log = log
	return p
}
func (p *TotalParam) WithHandler(countHandler CountHandler) *TotalParam {
	p.countHandler = countHandler
	return p
}

func (p *TotalParam) AppendFields(fields ...*Field) *TotalParam {
	p._Fields.Append(fields...)
	return p
}

func (p TotalParam) ToSQL() (sql string, err error) {
	fs := p._Fields.Copy() // 使用复制变量,后续正对场景的舒适化处理不会影响原始变量
	fs.SetSceneIfEmpty(SCENE_SQL_SELECT)
	where, err := fs.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.DialectWrapper().From(p._Table.Table()).Where(where...).Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
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
	return p.countHandler(sql)
}

type PaginationParam struct {
	_Table            TableI
	_Fields           Fields
	paginationHandler PaginationHandler
}

func (p PaginationParam) AppendFields(fields ...*Field) PaginationParam {
	p._Fields.Append(fields...)
	return p
}

func NewPaginationBuilder(tableName string) *PaginationParam {
	return &PaginationParam{
		_Table: TableFn(func() string { return tableName }),
	}
}
func (p *PaginationParam) WithHandler(paginationHandler PaginationHandler) *PaginationParam {
	p.paginationHandler = paginationHandler
	return p
}

func (p PaginationParam) ToSQL() (totalSql string, listSql string, err error) {
	table := p._Table.Table()
	totalSql, err = NewTotalBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", err
	}
	listSql, err = NewListBuilder(table).AppendFields(p._Fields...).ToSQL()
	if err != nil {
		return "", "", err
	}
	return totalSql, listSql, nil
}

func (p PaginationParam) Pagination(result any) (count int64, err error) {
	totalSql, listSql, err := p.ToSQL()
	if err != nil {
		return 0, err
	}
	return p.paginationHandler(totalSql, listSql, result)
}

type SetParam struct {
	_Table                  TableI
	_Fields                 Fields
	queryHandler            QueryHandler
	insertWithLastIdHandler InsertWithLastIdHandler
	execHandler             ExecWithRowsAffectedHandler
}

func (p SetParam) AppendFields(fields ...*Field) SetParam {
	p._Fields.Append(fields...)
	return p
}

func NewSetBuilder(tableName string) *SetParam {
	return &SetParam{
		_Table: TableFn(func() string { return tableName }),
	}
}

func (p *SetParam) WithHandler(queryHandler QueryHandler, insertWithLastIdHandler InsertWithLastIdHandler, execHandler ExecWithRowsAffectedHandler) *SetParam {
	p.queryHandler = queryHandler
	p.insertWithLastIdHandler = insertWithLastIdHandler
	p.execHandler = execHandler

	return p
}

// ToSQL 一次生成 查询、新增、修改 sql,若查询后记录存在,并且需要根据数据库记录值修改数据,则可以重新赋值后生成sql
func (p SetParam) ToSQL() (existsSql string, insertSql string, updateSql string, err error) {
	table := p._Table.Table()
	existsSql, err = NewExistsBuilder(table).AppendFields(p._Fields...).ToSQL() // 有些根据场景设置 如枚举值 ""，所有需要复制
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

func (p SetParam) Set() (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	table := p._Table.Table()
	exists, err := NewExistsBuilder(table).AppendFields(p._Fields...).Exists()
	if err != nil {
		return false, 0, 0, err
	}
	if exists {
		rowsAffected, err = NewUpdateBuilder(table).AppendFields(p._Fields...).ExecWithRowsAffected()
	} else {
		lastInsertId, rowsAffected, err = NewInsertBuilder(table).AppendFields(p._Fields...).InsertWithLastId()
	}
	isInsert = !exists
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
