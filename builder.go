package sqlbuilder

import (
	"reflect"
	"strings"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
)

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

// Dialect 设定驱动,方便直接使用
var Dialect = goqu.Dialect(Driver_sqlite3.String())

var Dialect_Mysql = goqu.Dialect(Driver_mysql.String())

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
}

func NewInsertBuilder(tableName string) InsertParam {
	return InsertParam{
		_TableI: TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
	}
}

func (p InsertParam) AppendFields(fields ...*Field) InsertParam {
	p._Fields.Append(fields...)
	return p
}

func (p InsertParam) Data() (data any, err error) {
	return p._Fields.Data()
}

func (p InsertParam) ToSQL() (sql string, err error) {
	p._Fields.SetScene(SCENE_SQL_INSERT)
	rowData, err := p.Data()
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
	ds := Dialect.Insert(table).Rows(rowData)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type InsertParams []InsertParam

func (rows InsertParams) ToSQL() (sql string, err error) {
	data := make([]any, 0)
	table := ""
	for i, r := range rows {
		if i == 0 {
			table = r._TableI.Table()
		}
		rowData, err := r.Data()
		if err != nil {
			return "", err
		}
		if IsNil(rowData) {
			continue
		}
		data = append(data, rowData)
	}
	ds := Dialect.Insert(table).Rows(data...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type DeleteParam struct {
	_TableI      TableI
	deletedField *Field
	_Fields      Fields
}

func NewDeleteBuilder(tableName string, deletedField Field) DeleteParam {
	return DeleteParam{
		_TableI:      TableFn(func() string { return tableName }),
		deletedField: &deletedField,
		_Fields:      make(Fields, 0),
	}
}

func (p DeleteParam) AppendFields(fields ...*Field) DeleteParam {
	p._Fields.Append(fields...)
	return p
}

func (p DeleteParam) Data() (data any, err error) {
	return p.deletedField.Data(p._Fields...)
}

func (p DeleteParam) Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p DeleteParam) ToSQL() (sql string, err error) {
	p._Fields.SetScene(SCENE_SQL_DELETE)
	data, err := p.Data()
	if err != nil {
		return "", err
	}

	where, err := p.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.Update(p._TableI.Table()).Set(data).Where(where...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type UpdateParam struct {
	_TableI TableI
	_Fields Fields
}

func NewUpdateBuilder(tableName string) UpdateParam {
	return UpdateParam{
		_TableI: TableFn(func() string { return tableName }),
		_Fields: make(Fields, 0),
	}
}

func (p UpdateParam) AppendFields(fields ...*Field) UpdateParam {
	p._Fields.Append(fields...)
	return p
}

func (p UpdateParam) Data() (data any, err error) {
	return p._Fields.Data()
}

func (p UpdateParam) Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p UpdateParam) ToSQL() (sql string, err error) {
	p._Fields.SetScene(SCENE_SQL_UPDATE)
	data, err := p.Data()
	if err != nil {
		return "", err
	}

	where, err := p.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.Update(p._TableI.Table()).Set(data).Where(where...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

// type FirstParamI interface {
// 	TableI
// 	_Select
// }

type FirstParam struct {
	_Table   TableI
	_columns []any
	_Fields  Fields
}

func (p FirstParam) AppendFields(fields ...*Field) FirstParam {
	p._Fields.Append(fields...)
	return p
}

func NewFirstBuilder(tableName string, columns ...any) FirstParam {
	return FirstParam{
		_Table:   TableFn(func() string { return tableName }),
		_columns: columns,
		_Fields:  make(Fields, 0),
	}
}

func (p FirstParam) Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p FirstParam) _Order() (orderedExpression []exp.OrderedExpression) {
	return p._Fields.Order()
}

func (p FirstParam) ToSQL() (sql string, err error) {
	p._Fields.SetScene(SCENE_SQL_SELECT)
	where, err := p.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.Select(p._columns...).
		From(p._Table.Table()).
		Where(where...).
		Order(p._Order()...).
		Limit(1)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type ListParam struct {
	_Table   TableI
	_columns []any
	_Fields  Fields
}

func (p ListParam) AppendFields(fields ...*Field) ListParam {
	p._Fields.Append(fields...)
	return p
}

func NewListBuilder(tableName string, columns ...any) ListParam {
	return ListParam{
		_columns: columns,
		_Table:   TableFn(func() string { return tableName }),
	}
}

func (p ListParam) Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}
func (p ListParam) _Order() (orderedExpression []exp.OrderedExpression) {
	return p._Fields.Order()
}

// CustomSQL 自定义SQL，方便构造更复杂的查询语句，如 Group,Having 等
func (p ListParam) CustomSQL(sqlFn func(p ListParam, ds *goqu.SelectDataset) (newDs *goqu.SelectDataset, err error)) (sql string, err error) {
	p._Fields.SetScene(SCENE_SQL_SELECT)
	ds := Dialect.Select(p._columns...)
	ds, err = sqlFn(p, ds)
	if err != nil {
		return "", err
	}
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

func (p ListParam) ToSQL() (sql string, err error) {
	p._Fields.SetScene(SCENE_SQL_SELECT)
	where, err := p.Where()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := p._Fields.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.Select(p._columns...).
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
	return sql, nil
}

type TotalParam struct {
	_Table  TableI
	_Fields Fields
}

func NewTotalBuilder(tableName string) TotalParam {
	return TotalParam{
		_Table: TableFn(func() string { return tableName }),
	}
}

func (p TotalParam) AppendFields(fields ...*Field) TotalParam {
	p._Fields.Append(fields...)
	return p
}

func (p TotalParam) Where() (expressions Expressions, err error) {
	return p._Fields.Where()
}

func (p TotalParam) ToSQL() (sql string, err error) {
	p._Fields.SetScene(SCENE_SQL_SELECT)
	where, err := p.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.From(p._Table.Table()).Where(where...).Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
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
