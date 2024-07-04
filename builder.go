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

const (
	Dialect_mysql   Driver = "mysql"
	Dialect_sqlite3 Driver = "sqlite3"
)

// Dialect 设定驱动,方便直接使用
var Dialect = goqu.Dialect(Dialect_sqlite3.String())

var Dialect_Mysql = goqu.Dialect(Dialect_mysql.String())

type Table interface {
	Table() (table string)
}
type TableFn func() (table string)

func (fn TableFn) Table() (table string) {
	return fn()
}

type Where interface {
	Where() (expressions []goqu.Expression, err error) // 容许在构建where函数时验证必要参数返回报错
}

type WhereFn func() (expressions []goqu.Expression, err error)

func (fn WhereFn) Where() (expressions []goqu.Expression, err error) {
	return fn()
}

type WhereSet []Where

type PaginationFn func() (index int, size int)

type _Pagination interface {
	Pagination() (index int, size int)
}

type Order interface {
	Order() (orderedExpressions []exp.OrderedExpression)
}

type OrderFn func() (orderedExpressions []exp.OrderedExpression)

func (fn OrderFn) Order() (orderedExpressions []exp.OrderedExpression) {
	return fn()
}

type OrderSet []Order

type _Select interface {
	Select() (columns []any)
}

type Data interface {
	Data() (data any, err error) //容许验证参数返回错误
}

type DataFn func() (any, error)

func (fn DataFn) Data() (data any, err error) {
	return fn()
}

type DataSet []Data

func ConcatOrderedExpression(orderedExpressions ...exp.OrderedExpression) []exp.OrderedExpression {
	return orderedExpressions
}

func ConcatExpression(expressions ...exp.Expression) []exp.Expression {
	return expressions
}

// InsertParam 供子类复用,修改数据
type InsertParam struct {
	_TableI  Table
	_DataSet DataSet
}

func NewInsertBuilder(table Table) InsertParam {
	return InsertParam{
		_TableI:  table,
		_DataSet: make(DataSet, 0),
	}
}

func (p InsertParam) _Copy() InsertParam {
	return InsertParam{
		_TableI:  p._TableI,
		_DataSet: p._DataSet,
	}
}
func (p InsertParam) Merge(insertParams ...InsertParam) InsertParam {
	newP := p._Copy()
	for _, up := range insertParams {
		newP._DataSet = append(newP._DataSet, up._DataSet...)
	}
	return newP
}

func (p InsertParam) AppendData(dataSet ...Data) InsertParam {
	newP := p._Copy()
	newP._DataSet = append(newP._DataSet, dataSet...)
	return newP
}

func (p InsertParam) _Data() (data any, err error) {
	return MergeData(p._DataSet...)
}

func (p InsertParam) ToSQL() (sql string, err error) {
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
		rowData, err := r._Data()
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

type UpdateParam struct {
	_TableI   Table
	_DataSet  DataSet  //中间件的Data 集合
	_WhereSet WhereSet //中间件的Where 集合
}

func NewUpdateBuilder(table Table) UpdateParam {
	return UpdateParam{
		_TableI:   table,
		_DataSet:  make(DataSet, 0),
		_WhereSet: make(WhereSet, 0),
	}
}

func (p UpdateParam) _Copy() UpdateParam {
	return UpdateParam{
		_TableI:   p._TableI,
		_DataSet:  p._DataSet,
		_WhereSet: p._WhereSet,
	}
}

func (p UpdateParam) Merge(updateParams ...UpdateParam) UpdateParam {
	newP := p._Copy()
	for _, up := range updateParams {
		newP._DataSet = append(newP._DataSet, up._DataSet...)
		newP._WhereSet = append(newP._WhereSet, up._WhereSet...)
	}
	return newP
}

func (p UpdateParam) AppendData(dataSet ...Data) UpdateParam {
	newP := p._Copy()
	newP._DataSet = append(newP._DataSet, dataSet...)
	return newP
}

func (p UpdateParam) AppendWhere(whereSet ...Where) UpdateParam {
	newP := p._Copy()
	newP._WhereSet = append(newP._WhereSet, whereSet...)
	return newP
}
func (p UpdateParam) _Data() (data any, err error) {
	return MergeData(p._DataSet...)
}

func (p UpdateParam) _Where() (expressions []goqu.Expression, err error) {
	return MergeWhere(p._WhereSet...)
}

func (p UpdateParam) ToSQL() (sql string, err error) {
	data, err := p._Data()
	if err != nil {
		return "", err
	}
	where, err := p._Where()
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

type FirstParamI interface {
	Table
	_Select
	//Where
	//Order
}

type FirstParam struct {
	_FirstParamI FirstParamI
	_WhereSet    WhereSet //中间件的Where 集合
	_OrderSet    OrderSet
}

func (p FirstParam) _Copy() FirstParam {
	return FirstParam{
		_FirstParamI: p._FirstParamI,
		_WhereSet:    p._WhereSet,
		_OrderSet:    p._OrderSet,
	}
}
func (p FirstParam) Merge(firstParams ...FirstParam) FirstParam {
	newP := p._Copy()
	for _, up := range firstParams {
		newP._WhereSet = append(newP._WhereSet, up._WhereSet...)
		newP._OrderSet = append(newP._OrderSet, up._OrderSet...)
	}
	return newP
}

func NewFirstBuilder(firstParamI FirstParamI) FirstParam {
	return FirstParam{
		_FirstParamI: firstParamI,
		_WhereSet:    make(WhereSet, 0),
	}
}

func (p FirstParam) AppendWhere(whereSet ...Where) FirstParam {
	newP := p._Copy()
	newP._WhereSet = append(newP._WhereSet, whereSet...)
	return newP
}
func (p FirstParam) _Where() (expressions []goqu.Expression, err error) {
	return MergeWhere(p._WhereSet...)
}

func (p FirstParam) AppendOrder(orderSet ...Order) FirstParam {
	newP := p._Copy()
	newP._OrderSet = append(newP._OrderSet, orderSet...)
	return newP
}

func (p FirstParam) _Order() (orderedExpression []exp.OrderedExpression) {
	return MergeOrder(p._OrderSet...)
}

func (p FirstParam) ToSQL() (sql string, err error) {
	where, err := p._Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.Select(p._FirstParamI.Select()...).
		From(p._FirstParamI.Table()).
		Where(where...).
		Order(p._Order()...).
		Limit(1)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type ListParamI interface {
	Table
	_Select
	_Pagination
}

type ListParam struct {
	_ListParamI ListParamI
	_WhereSet   WhereSet
	_OrderSet   OrderSet
}

func (p ListParam) _Copy() ListParam {
	return ListParam{
		_ListParamI: p._ListParamI,
		_WhereSet:   p._WhereSet,
		_OrderSet:   p._OrderSet,
	}
}
func (p ListParam) Merge(listParams ...ListParam) ListParam {
	newP := p._Copy()
	for _, up := range listParams {
		newP._WhereSet = append(newP._WhereSet, up._WhereSet...)
		newP._OrderSet = append(newP._OrderSet, up._OrderSet...)
	}
	return newP
}

func NewListBuilder(listParamI ListParamI) ListParam {
	return ListParam{
		_ListParamI: listParamI,
		_WhereSet:   make(WhereSet, 0),
	}
}
func (p ListParam) AppendWhere(whereSet ...Where) ListParam {
	newP := p._Copy()
	newP._WhereSet = append(newP._WhereSet, whereSet...)
	return newP
}

func (p ListParam) AppendOrder(orderSet ...Order) ListParam {
	newP := p._Copy()
	newP._OrderSet = append(newP._OrderSet, orderSet...)
	return newP
}

func (p ListParam) _Where() (expressions []goqu.Expression, err error) {
	return MergeWhere(p._WhereSet...)
}
func (p ListParam) _Order() (orderedExpression []exp.OrderedExpression) {
	return MergeOrder(p._OrderSet...)
}

// CustomSQL 自定义SQL，方便构造更复杂的查询语句，如 Group,Having 等
func (p ListParam) CustomSQL(sqlFn func(p ListParam, ds *goqu.SelectDataset) (newDs *goqu.SelectDataset, err error)) (sql string, err error) {
	ds := Dialect.Select()
	ds, err = sqlFn(p._Copy(), ds)
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
	where, err := p._Where()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := p._ListParamI.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.Select(p._ListParamI.Select()...).
		From(p._ListParamI.Table()).
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
	_Table    Table
	_WhereSet WhereSet
}

func NewTotalBuilder(table Table) TotalParam {
	return TotalParam{
		_Table:    table,
		_WhereSet: make(WhereSet, 0),
	}
}

func (p TotalParam) _Copy() TotalParam {
	return TotalParam{
		_Table:    p._Table,
		_WhereSet: p._WhereSet,
	}
}
func (p TotalParam) Merge(totalParams ...TotalParam) TotalParam {
	newP := p._Copy()
	for _, up := range totalParams {
		newP._WhereSet = append(newP._WhereSet, up._WhereSet...)
	}
	return newP
}

func (p TotalParam) AppendWhere(whereSet ...Where) TotalParam {
	newP := p._Copy()
	newP._WhereSet = append(newP._WhereSet, whereSet...)
	return newP
}

func (p TotalParam) Where() (expressions []goqu.Expression, err error) {
	return MergeWhere(p._WhereSet...)
}

func (p TotalParam) ToSQL() (sql string, err error) {
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

func MergeData(dataIs ...Data) (newData map[string]any, err error) {
	newData = map[string]any{}
	for _, dataI := range dataIs {
		if IsNil(dataI) {
			continue
		}
		data, err := dataI.Data()
		if err != nil {
			return newData, err
		}
		if IsNil(data) {
			continue
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

func MergeWhere(whereIs ...Where) (mergedWhere []goqu.Expression, err error) {
	expressions := make([]goqu.Expression, 0)
	for _, whereI := range whereIs {
		if IsNil(whereI) {
			continue
		}
		where, err := whereI.Where()
		if err != nil {
			return expressions, err
		}
		expressions = append(expressions, where...)
	}

	return expressions, nil
}

func MergeOrder(orders ...Order) (orderedExpressions []exp.OrderedExpression) {
	orderedExpressions = make([]exp.OrderedExpression, 0)
	for _, o := range orders {
		orderedExpressions = append(orderedExpressions, o.Order()...)
	}
	return orderedExpressions
}
