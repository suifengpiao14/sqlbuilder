package sqlbuilder

import (
	"reflect"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
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

type Where interface {
	Where() (expressions []goqu.Expression, err error) // 容许在构建where函数时验证必要参数返回报错
}

type WhereSet []Where

type _Pagination interface {
	Pagination() (index int, size int)
}

type _Order interface {
	Order() (orderedExpressions []exp.OrderedExpression)
}

type _Select interface {
	Select() (columns []interface{})
}

type Data interface {
	Data() (data interface{}, err error) //容许验证参数返回错误
}

type DataSet []Data

type InsertParamI interface {
	_Tabale
	Data
}

// InsertParam 供子类复用,修改数据
type InsertParam struct {
	InsertParamI
	_DataSet DataSet
}

func (p *InsertParam) AppendData(dataSet ...Data) *InsertParam {
	if p._DataSet == nil {
		p._DataSet = make(DataSet, 0)
	}
	p._DataSet = append(p._DataSet, dataSet...)
	return p
}

func (p InsertParam) Data() (data interface{}, err error) {
	dataIs := make(DataSet, 0)
	dataIs = append(dataIs, p.InsertParamI)
	dataIs = append(dataIs, p._DataSet...)
	return MergeData(dataIs...)
}

func Insert(rows ...InsertParam) (sql string, err error) {
	data := make([]interface{}, 0)
	table := ""
	for i, r := range rows {
		if i == 0 {
			table = r.Table()
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

func MergeWhere(whereIs ...Where) (mergedWhere []goqu.Expression, err error) {
	expressions := make([]goqu.Expression, 0)
	for _, whereI := range whereIs {
		where, err := whereI.Where()
		if err != nil {
			return expressions, err
		}
		expressions = append(expressions, where...)
	}

	return expressions, nil
}

type UpdateParamI interface {
	_Tabale
	Where
	Data
}

type UpdateParam struct {
	UpdateParamI
	_DataSet  DataSet  //中间件的Data 集合
	_WhereSet WhereSet //中间件的Where 集合
}

func (p *UpdateParam) AppendData(dataSet ...Data) *UpdateParam {
	if p._DataSet == nil {
		p._DataSet = make(DataSet, 0)
	}
	p._DataSet = append(p._DataSet, dataSet...)
	return p
}

func (p *UpdateParam) AppendWhere(whereSet ...Where) *UpdateParam {
	if p._WhereSet == nil {
		p._WhereSet = make(WhereSet, 0)
	}
	p._WhereSet = append(p._WhereSet, whereSet...)
	return p
}
func (p UpdateParam) Data() (data interface{}, err error) {
	dataIs := make(DataSet, 0)
	dataIs = append(dataIs, p.UpdateParamI)
	dataIs = append(dataIs, p._DataSet...)
	return MergeData(dataIs...)
}

func (p UpdateParam) Where() (expressions []goqu.Expression, err error) {
	whereIs := make([]Where, 0)
	whereIs = append(whereIs, p.UpdateParamI)
	whereIs = append(whereIs, p._WhereSet...)
	return MergeWhere(whereIs...)
}

func Update(param UpdateParam) (sql string, err error) {
	data, err := param.Data()
	if err != nil {
		return "", err
	}
	where, err := param.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.Update(param.Table()).Set(data).Where(where...)
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
	Where
	_Order
}

type FirstParam struct {
	FirstParamI
	_WhereSet WhereSet //中间件的Where 集合
}

func (p *FirstParam) AppendWhere(whereSet ...Where) *FirstParam {
	if p._WhereSet == nil {
		p._WhereSet = make(WhereSet, 0)
	}
	p._WhereSet = append(p._WhereSet, whereSet...)
	return p
}
func (p FirstParam) Where() (expressions []goqu.Expression, err error) {
	whereIs := make([]Where, 0)
	whereIs = append(whereIs, p.FirstParamI)
	whereIs = append(whereIs, p._WhereSet...)
	return MergeWhere(whereIs...)
}

func First(param FirstParamI) (sql string, err error) {
	where, err := param.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.Select(param.Select()...).From(param.Table()).Where(where...).Order(param.Order()...).Limit(1)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type ListParamI interface {
	_Tabale
	_Select
	Where
	_Pagination
	_Order
}

type ListParam struct {
	ListParamI
	_WhereSet WhereSet
}

func (p *ListParam) AppendWhere(whereSet ...Where) *ListParam {
	if p._WhereSet == nil {
		p._WhereSet = make(WhereSet, 0)
	}
	p._WhereSet = append(p._WhereSet, whereSet...)
	return p
}

func (p ListParam) Where() (expressions []goqu.Expression, err error) {
	whereIs := make([]Where, 0)
	whereIs = append(whereIs, p.ListParamI)
	whereIs = append(whereIs, p._WhereSet...)
	return MergeWhere(whereIs...)
}

func List(param ListParamI) (sql string, err error) {
	where, err := param.Where()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := param.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.Select(param.Select()...).From(param.Table()).Where(where...).Order(param.Order()...)
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
	Where
}
type TotalParam struct {
	TotalParamI
	_WhereSet WhereSet
}

func (p *TotalParam) AppendWhere(whereSet ...Where) *TotalParam {
	if p._WhereSet == nil {
		p._WhereSet = make(WhereSet, 0)
	}
	p._WhereSet = append(p._WhereSet, whereSet...)
	return p
}
func (p TotalParam) Where() (expressions []goqu.Expression, err error) {
	whereIs := make([]Where, 0)
	whereIs = append(whereIs, p.TotalParamI)
	whereIs = append(whereIs, p._WhereSet...)
	return MergeWhere(whereIs...)
}

func Total(param TotalParamI) (sql string, err error) {
	where, err := param.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.From(param.Table()).Where(where...).Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

func MergeData(dataIs ...Data) (newData map[string]interface{}, err error) {
	newData = map[string]interface{}{}
	for _, dataI := range dataIs {
		data, err := dataI.Data()
		if err != nil {
			return newData, err
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
			return nil, errors.Errorf("unsupported update interface type %+v", rv.Type())
		}
	}

	return newData, nil
}
