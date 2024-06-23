package goqucrud

import (
	"reflect"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
)

const (
	Dialect_mysql   = "mysql"
	Dialect_sqlite3 = "sqlite3"
)

// Dialect 设定驱动,方便直接使用
var Dialect = goqu.Dialect(Dialect_sqlite3)

var Dialect_Mysql = goqu.Dialect(Dialect_mysql)

type _Tabale interface {
	Table() (table string)
}

type _Where interface {
	Where() (expressions []goqu.Expression)
}

type _Pagination interface {
	Pagination() (index int, size int)
}

type _Order interface {
	Order() (orderedExpressions []exp.OrderedExpression)
}

type _Select interface {
	Select() (columns []interface{})
}

type _Validate interface {
	Validate() (err error)
}

type _Data interface {
	Data() (data interface{}, err error)
}

type InsertParamI interface {
	_Tabale
	_Data
	_Validate
}

// InsertParam 供子类复用,修改数据
type InsertParam struct {
	InsertParamI
	ExtraData map[string]any
}

func (p InsertParam) Data() (data interface{}, err error) {
	return mergeData(p.InsertParamI, p.ExtraData)
}

func mergeData(dataI _Data, extraData map[string]any) (out map[string]any, err error) {
	oldData, err := dataI.Data()
	if err != nil {
		return nil, err
	}
	mergedData, err := MergeAnyData(oldData, extraData)
	if err != nil {
		return nil, err
	}
	return mergedData, nil
}

func Insert(rows ...InsertParamI) (sql string, err error) {
	data := make([]interface{}, 0)
	table := ""
	for i, r := range rows {
		if i == 0 {
			table = r.Table()
		}
		if err = r.Validate(); err != nil {
			return "", err
		}
		rowData, err := r.Data()
		if err != nil {
			return "", err
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

func mergeWhere(whereI _Where, extraWhere []goqu.Expression) (mergedWhere []goqu.Expression) {
	expressions := whereI.Where()
	if expressions == nil {
		expressions = make([]goqu.Expression, 0)
	}
	expressions = append(expressions, extraWhere...)
	return expressions
}

type UpdateParamI interface {
	_Tabale
	_Where
	_Data
	_Validate
}

type UpdateParam struct {
	UpdateParamI
	ExtraData  map[string]any
	ExtraWhere []goqu.Expression
}

func (p UpdateParam) Data() (data interface{}, err error) {
	return mergeData(p.UpdateParamI, p.ExtraData)
}

func (p UpdateParam) Where() (expressions []goqu.Expression) {
	return mergeWhere(p.UpdateParamI, p.ExtraWhere)
}

func Update(param UpdateParamI) (sql string, err error) {
	if err = param.Validate(); err != nil {
		return "", err
	}
	data, err := param.Data()
	if err != nil {
		return "", err
	}
	ds := Dialect.Update(param.Table()).Set(data).Where(param.Where()...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

var SorftDelete = Update

type FirstParamI interface {
	_Tabale
	_Select
	_Where
	_Order
}

type FirstParam struct {
	FirstParamI
	ExtraWhere []goqu.Expression
}

func (p FirstParam) Where() (expressions []goqu.Expression) {
	return mergeWhere(p.FirstParamI, p.ExtraWhere)
}

func First(param FirstParamI) (sql string, err error) {
	ds := Dialect.Select(param.Select()...).From(param.Table()).Where(param.Where()...).Order(param.Order()...).Limit(1)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type ListParamI interface {
	_Tabale
	_Select
	_Where
	_Pagination
	_Order
}

type ListParam struct {
	ListParamI
	ExtraWhere []goqu.Expression
}

func (p ListParam) Where() (expressions []goqu.Expression) {
	return mergeWhere(p.ListParamI, p.ExtraWhere)
}

func List(param ListParamI) (sql string, err error) {
	expressions := param.Where()
	pageIndex, pageSize := param.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.Select(param.Select()...).From(param.Table()).Where(expressions...).Order(param.Order()...)
	if pageSize > 0 {
		ds = ds.Offset(uint(ofsset)).Limit(uint(pageSize))
	}
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type TotalParamI interface {
	_Tabale
	_Where
}
type TotalParam struct {
	TotalParamI
	ExtraWhere []goqu.Expression
}

func (p TotalParam) Where() (expressions []goqu.Expression) {
	return mergeWhere(p.TotalParamI, p.ExtraWhere)
}

func Total(param TotalParamI) (sql string, err error) {
	expressions := param.Where()
	ds := Dialect.From(param.Table()).Where(expressions...).Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

func MergeAnyData(datas ...interface{}) (newData map[string]interface{}, err error) {
	newData = map[string]interface{}{}
	for _, data := range datas {
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
			return nil, errors.Errorf("unsupported update interface type %+v", rv.Type())
		}
	}

	return
}
