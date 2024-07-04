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

type TableI interface {
	Table() (table string)
}
type TableFn func() (table string)

func (fn TableFn) Table() (table string) {
	return fn()
}

type WhereI interface {
	Where() (expressions []goqu.Expression, err error) // 容许在构建where函数时验证必要参数返回报错
}

type WhereFn func() (expressions []goqu.Expression, err error)

func (fn WhereFn) Where() (expressions []goqu.Expression, err error) {
	return fn()
}

type PaginationFn func() (index int, size int)

type PaginationI interface {
	Pagination() (index int, size int)
}

type OrderI interface {
	Order() (orderedExpressions []exp.OrderedExpression)
}

type SelectI interface {
	Select() (columns []any)
}

type DataI interface {
	Data() (data any, err error) //容许验证参数返回错误
}

type DataFn func() (any, error)

func (fn DataFn) Data() (data any, err error) {
	return fn()
}

type InsertParamI interface {
	TableI
	DataI
}

type InsertParamIs []InsertParamI

func NewInsertBuilder(subPs ...InsertParamI) (builder InsertParamIs) {
	builder = InsertParamIs{}
	builder.Append(subPs...)
	return builder
}
func (ps InsertParamIs) Append(subPs ...InsertParamI) (newPs InsertParamIs) {
	newPs = make(InsertParamIs, 0)
	newPs = append(newPs, ps...)
	newPs = append(newPs, subPs...)
	return newPs
}

func (ps InsertParamIs) ToSQL() (rawSql string, err error) {
	tables := make([]TableI, 0)
	for _, p := range ps {
		tables = append(tables, p)
	}
	var table any
	allTable := MergeTable(tables...)

	for _, t := range allTable { // 取第一个
		table = t
		break
	}
	dataIs := make([]DataI, 0)
	for _, p := range ps {
		dataIs = append(dataIs, p)
	}
	data, err := MergeData(dataIs...)
	if err != nil {
		return "", err
	}
	ds := Dialect.Insert(table).Rows(data)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

type UpdateParamI interface {
	TableI
	DataI
	WhereI
}

type UpdateParamIs []UpdateParamI

func NewUpdateBuilder(subPs ...UpdateParamI) (builder UpdateParamIs) {
	builder = UpdateParamIs{}
	builder.Append(subPs...)
	return builder
}
func (ps UpdateParamIs) Append(subPs ...UpdateParamI) (newPs UpdateParamIs) {
	newPs = make(UpdateParamIs, 0)
	newPs = append(newPs, ps...)
	newPs = append(newPs, subPs...)
	return newPs
}

func (ps UpdateParamIs) ToSQL() (rawSql string, err error) {
	tables := make([]TableI, 0)
	for _, p := range ps {
		tables = append(tables, p)
	}
	var table any
	allTable := MergeTable(tables...)

	for _, t := range allTable { // 取第一个
		table = t
		break
	}

	dataIs := make([]DataI, 0)
	for _, p := range ps {
		dataIs = append(dataIs, p)
	}
	data, err := MergeData(dataIs...)
	if err != nil {
		return "", err
	}

	whereIs := make([]WhereI, 0)
	for _, p := range ps {
		whereIs = append(whereIs, p)
	}
	expressions, err := MergeWhere(whereIs...)
	if err != nil {
		return "", err
	}

	ds := Dialect.Update(table).Set(data).Where(expressions...)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

type FirstParamI interface {
	TableI
	SelectI
	WhereI
	OrderI
}

type FirstParamIs []FirstParamI

func NewFirstBuilder(subPs ...FirstParamI) (builder FirstParamIs) {
	builder = FirstParamIs{}
	builder.Append(subPs...)
	return builder
}
func (ps FirstParamIs) Append(subPs ...FirstParamI) (newPs FirstParamIs) {
	newPs = make(FirstParamIs, 0)
	newPs = append(newPs, ps...)
	newPs = append(newPs, subPs...)
	return newPs
}

func (ps FirstParamIs) ToSQL() (rawSql string, err error) {
	whereIs := make([]WhereI, 0)
	for _, p := range ps {
		whereIs = append(whereIs, p)
	}
	expressions, err := MergeWhere(whereIs...)
	if err != nil {
		return "", err
	}
	selectIs := make([]SelectI, 0)
	for _, p := range ps {
		selectIs = append(selectIs, p)
	}
	columns := MergeSelect(selectIs...)

	tables := make([]TableI, 0)
	for _, p := range ps {
		tables = append(tables, p)
	}
	allTable := MergeTable(tables...)
	orderIs := make([]OrderI, 0)
	for _, p := range ps {
		orderIs = append(orderIs, p)
	}
	allOrder := MergeOrder(orderIs...)

	ds := Dialect.Select(columns).
		From(allTable...).
		Where(expressions...).
		Order(allOrder...).
		Limit(1)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}

	return rawSql, nil
}

type ListParamI interface {
	TableI
	SelectI
	WhereI
	PaginationI
	OrderI
}

type ListParamIs []ListParamI

func NewListBuilder(subPs ...ListParamI) (builder ListParamIs) {
	builder = ListParamIs{}
	builder.Append(subPs...)
	return builder
}
func (ps ListParamIs) Append(subPs ...ListParamI) (newPs ListParamIs) {
	newPs = make(ListParamIs, 0)
	newPs = append(newPs, ps...)
	newPs = append(newPs, subPs...)
	return newPs
}
func (ps ListParamIs) ToSQL() (rawSql string, err error) {
	whereIs := make([]WhereI, 0)
	for _, p := range ps {
		whereIs = append(whereIs, p)
	}
	expressions, err := MergeWhere(whereIs...)
	if err != nil {
		return "", err
	}
	selectIs := make([]SelectI, 0)
	for _, p := range ps {
		selectIs = append(selectIs, p)
	}
	columns := MergeSelect(selectIs...)

	tables := make([]TableI, 0)
	for _, p := range ps {
		tables = append(tables, p)
	}
	allTable := MergeTable(tables...)
	orderIs := make([]OrderI, 0)
	for _, p := range ps {
		orderIs = append(orderIs, p)
	}
	allOrder := MergeOrder(orderIs...)

	paginationIs := make([]PaginationI, 0)
	for _, p := range ps {
		paginationIs = append(paginationIs, p)
	}
	pageIndex, pageSize := MergePagination(paginationIs...)
	ofsset := pageIndex * pageSize
	ds := Dialect.Select(columns...).
		From(allTable...).
		Where(expressions...).
		Order(allOrder...)
	if pageSize > 0 {
		ds = ds.Offset(uint(ofsset)).Limit(uint(pageSize))
	}
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil

}

// CustomSQL 自定义SQL，方便构造更复杂的查询语句，如 Group,Having 等
func (ps ListParamIs) CustomSQL(sqlFn func(p ListParamIs, ds *goqu.SelectDataset) (newDs *goqu.SelectDataset, err error)) (sql string, err error) {
	ds := Dialect.Select()
	ds, err = sqlFn(ps, ds)
	if err != nil {
		return "", err
	}
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type TotalParamI interface {
	TableI
	WhereI
}

type TotalParamIs []TotalParamI

func NewTotalBuilder(subPs ...TotalParamI) (builder TotalParamIs) {
	builder = TotalParamIs{}
	builder.Append(subPs...)
	return builder
}
func (ps TotalParamIs) Append(subPs ...TotalParamI) (newPs TotalParamIs) {
	newPs = make(TotalParamIs, 0)
	newPs = append(newPs, ps...)
	newPs = append(newPs, subPs...)
	return newPs
}
func (ps TotalParamIs) ToSQL() (rawSql string, err error) {
	whereIs := make([]WhereI, 0)
	for _, p := range ps {
		whereIs = append(whereIs, p)
	}
	expressions, err := MergeWhere(whereIs...)
	if err != nil {
		return "", err
	}

	tables := make([]TableI, 0)
	for _, p := range ps {
		tables = append(tables, p)
	}
	allTable := MergeTable(tables...)

	ds := Dialect.Select(goqu.COUNT(goqu.Star()).As("count")).
		From(allTable...).
		Where(expressions...)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil

}

func MergeData(dataIs ...DataI) (newData map[string]any, err error) {
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

func MergeWhere(whereIs ...WhereI) (mergedWhere []goqu.Expression, err error) {
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

func MergeSelect(selectIs ...SelectI) (allColumn []any) {
	allColumn = make([]any, 0)
	for _, selectI := range selectIs {
		if IsNil(selectI) {
			continue
		}
		columns := selectI.Select()
		allColumn = append(allColumn, columns...)
	}

	return allColumn
}
func MergeTable(tables ...TableI) (allTable []any) {
	allTable = make([]any, 0)
	for _, selectI := range tables {
		if IsNil(selectI) {
			continue
		}
		table := selectI.Table()
		if table == "" {
			continue
		}
		allTable = append(allTable, table)
	}
	return allTable
}
func MergeOrder(orderIs ...OrderI) (allOrder []exp.OrderedExpression) {
	allOrder = make([]exp.OrderedExpression, 0)
	for _, orderI := range orderIs {
		if IsNil(orderI) {
			continue
		}
		allOrder = append(allOrder, orderI.Order()...)
	}
	return allOrder
}

func MergePagination(paginationIs ...PaginationI) (index int, size int) {
	for _, paginationI := range paginationIs {
		if IsNil(paginationI) {
			continue
		}
		index, size = paginationI.Pagination()
		if index < 0 {
			index = 0
		}
		if size < 0 {
			size = 0
		}
		return index, size
	}
	return 0, 0
}
func ConcatOrderedExpression(orderedExpressions ...exp.OrderedExpression) []exp.OrderedExpression {
	return orderedExpressions
}

func ConcatExpression(expressions ...exp.Expression) []exp.Expression {
	return expressions
}
