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

type _Tabale interface {
	Table() (table string)
}

type Where interface {
	Where() (expressions []goqu.Expression, err error) // 容许在构建where函数时验证必要参数返回报错
}

type WhereFn func() (expressions []goqu.Expression, err error)

func (fn WhereFn) Where() (expressions []goqu.Expression, err error) {
	return fn()
}

type WhereSet []Where

type _Pagination interface {
	Pagination() (index int, size int)
}

type _Order interface {
	Order() (orderedExpressions []exp.OrderedExpression)
}

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

type InsertParamI interface {
	_Tabale
	Data
}

func ConcatOrderedExpression(orderedExpressions ...exp.OrderedExpression) []exp.OrderedExpression {
	return orderedExpressions
}

func ConcatExpression(expressions ...exp.Expression) []exp.Expression {
	return expressions
}

// InsertParam 供子类复用,修改数据
type InsertParam struct {
	_InsertParamI InsertParamI
	_DataSet      DataSet
}

func NewInsertBuilder(insertParamI InsertParamI) InsertParam {
	return InsertParam{
		_InsertParamI: insertParamI,
		_DataSet:      make(DataSet, 0),
	}
}

func (p InsertParam) Copy() InsertParam {
	return InsertParam{
		_InsertParamI: p._InsertParamI,
		_DataSet:      p._DataSet,
	}
}
func (p InsertParam) Merge(insertParams ...InsertParam) InsertParam {
	newP := p.Copy()
	for _, up := range insertParams {
		newP._DataSet = append(newP._DataSet, up._DataSet...)
	}
	return newP
}

func (p InsertParam) AppendData(dataSet ...Data) InsertParam {
	newP := p.Copy()
	newP._DataSet = append(newP._DataSet, p._DataSet...)
	newP._DataSet = append(newP._DataSet, dataSet...)
	return newP
}

func (p InsertParam) Data() (data any, err error) {
	dataIs := make(DataSet, 0)
	dataIs = append(dataIs, p._InsertParamI)
	dataIs = append(dataIs, p._DataSet...)
	return MergeData(dataIs...)
}

func (p InsertParam) ToSQL() (sql string, err error) {
	rowData, err := p.Data()
	if err != nil {
		return "", err
	}
	if IsNil(rowData) {
		err = errors.New("InsertParam.Data() return nil data")
		return "", err
	}
	table := p._InsertParamI.Table()
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
			table = r._InsertParamI.Table()
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
	_UpdateParamI UpdateParamI
	_DataSet      DataSet  //中间件的Data 集合
	_WhereSet     WhereSet //中间件的Where 集合
}

func NewUpdateBuilder(updateParamI UpdateParamI) UpdateParam {
	return UpdateParam{
		_UpdateParamI: updateParamI,
		_DataSet:      make(DataSet, 0),
		_WhereSet:     make(WhereSet, 0),
	}
}

func (p UpdateParam) Copy() UpdateParam {
	return UpdateParam{
		_UpdateParamI: p._UpdateParamI,
		_DataSet:      p._DataSet,
		_WhereSet:     p._WhereSet,
	}
}

func (p UpdateParam) Merge(updateParams ...UpdateParam) UpdateParam {
	newP := p.Copy()
	for _, up := range updateParams {
		newP._DataSet = append(newP._DataSet, up._DataSet...)
		newP._WhereSet = append(newP._WhereSet, up._WhereSet...)
	}
	return newP
}

func (p UpdateParam) AppendData(dataSet ...Data) UpdateParam {
	newP := p.Copy()
	newP._DataSet = append(newP._DataSet, p._DataSet...)
	newP._DataSet = append(newP._DataSet, dataSet...)
	return newP
}

func (p UpdateParam) AppendWhere(whereSet ...Where) UpdateParam {
	newP := p.Copy()
	newP._WhereSet = append(newP._WhereSet, p._WhereSet...)
	newP._WhereSet = append(newP._WhereSet, whereSet...)
	return newP
}
func (p UpdateParam) Data() (data any, err error) {
	dataIs := make(DataSet, 0)
	dataIs = append(dataIs, p._UpdateParamI)
	dataIs = append(dataIs, p._DataSet...)
	return MergeData(dataIs...)
}

func (p UpdateParam) Where() (expressions []goqu.Expression, err error) {
	whereIs := make([]Where, 0)
	whereIs = append(whereIs, p._UpdateParamI)
	whereIs = append(whereIs, p._WhereSet...)
	return MergeWhere(whereIs...)
}

func (p UpdateParam) ToSQL() (sql string, err error) {
	data, err := p.Data()
	if err != nil {
		return "", err
	}
	where, err := p.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.Update(p._UpdateParamI.Table()).Set(data).Where(where...)
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type FirstParamI interface {
	_Tabale
	_Select
	Where
	_Order
}

type FirstParam struct {
	_FirstParamI FirstParamI
	_WhereSet    WhereSet //中间件的Where 集合
}

func (p FirstParam) Copy() FirstParam {
	return FirstParam{
		_FirstParamI: p._FirstParamI,
		_WhereSet:    p._WhereSet,
	}
}
func (p FirstParam) Merge(firstParams ...FirstParam) FirstParam {
	newP := p.Copy()
	for _, up := range firstParams {
		newP._WhereSet = append(newP._WhereSet, up._WhereSet...)
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
	newP := p.Copy()
	newP._WhereSet = append(newP._WhereSet, p._WhereSet...)
	newP._WhereSet = append(newP._WhereSet, whereSet...)
	return newP
}
func (p FirstParam) Where() (expressions []goqu.Expression, err error) {
	whereIs := make([]Where, 0)
	whereIs = append(whereIs, p._FirstParamI)
	whereIs = append(whereIs, p._WhereSet...)
	return MergeWhere(whereIs...)
}

func (p FirstParam) ToSQL() (sql string, err error) {
	where, err := p.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.Select(p._FirstParamI.Select()...).
		From(p._FirstParamI.Table()).
		Where(where...).
		Order(p._FirstParamI.Order()...).
		Limit(1)
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
	_ListParamI ListParamI
	_WhereSet   WhereSet
}

func (p ListParam) Copy() ListParam {
	return ListParam{
		_ListParamI: p._ListParamI,
		_WhereSet:   p._WhereSet,
	}
}
func (p ListParam) Merge(listParams ...ListParam) ListParam {
	newP := p.Copy()
	for _, up := range listParams {
		newP._WhereSet = append(newP._WhereSet, up._WhereSet...)
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
	newP := p.Copy()
	newP._WhereSet = append(newP._WhereSet, p._WhereSet...)
	newP._WhereSet = append(newP._WhereSet, whereSet...)
	return newP
}

func (p ListParam) Where() (expressions []goqu.Expression, err error) {
	wheres := make([]Where, 0)
	wheres = append(wheres, p._ListParamI)
	wheres = append(wheres, p._WhereSet...)
	return MergeWhere(wheres...)
}

// CustomSQL 自定义SQL，方便构造更复杂的查询语句，如 Group,Having 等
func (p ListParam) CustomSQL(sqlFn func(p ListParam, ds *goqu.SelectDataset) (newDs *goqu.SelectDataset, err error)) (sql string, err error) {
	ds := Dialect.Select()
	ds, err = sqlFn(p.Copy(), ds)
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
	where, err := p.Where()
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
		Order(p._ListParamI.Order()...)
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
	_TotalParamI TotalParamI
	_WhereSet    WhereSet
}

func NewTotalBuilder(totalParamI TotalParamI) TotalParam {
	return TotalParam{
		_TotalParamI: totalParamI,
		_WhereSet:    make(WhereSet, 0),
	}
}

func (p TotalParam) Copy() TotalParam {
	return TotalParam{
		_TotalParamI: p._TotalParamI,
		_WhereSet:    p._WhereSet,
	}
}
func (p TotalParam) Merge(totalParams ...TotalParam) TotalParam {
	newP := p.Copy()
	for _, up := range totalParams {
		newP._WhereSet = append(newP._WhereSet, up._WhereSet...)
	}
	return newP
}

func (p TotalParam) AppendWhere(whereSet ...Where) TotalParam {
	newP := p.Copy()
	newP._WhereSet = append(newP._WhereSet, p._WhereSet...)
	newP._WhereSet = append(newP._WhereSet, whereSet...)
	return newP
}

func (p TotalParam) Where() (expressions []goqu.Expression, err error) {
	whereIs := make([]Where, 0)
	whereIs = append(whereIs, p._TotalParamI)
	whereIs = append(whereIs, p._WhereSet...)
	return MergeWhere(whereIs...)
}

func (p TotalParam) ToSQL() (sql string, err error) {
	where, err := p.Where()
	if err != nil {
		return "", err
	}
	ds := Dialect.From(p._TotalParamI.Table()).
		Where(where...).
		Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

func MergeData(dataIs ...Data) (newData map[string]any, err error) {
	newData = map[string]any{}
	for _, dataI := range dataIs {
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
