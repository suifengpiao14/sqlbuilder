package sqlbuilder

import (
	"github.com/doug-martin/goqu/v9"
)

type ColumnI interface {
	SelectColumns() (columns []any)
	SelectWhere() (expression []goqu.Expression, err error)
	InsertValue() (value any, err error)
	UpdateValue() (value any, err error)
	UpdateWhere() (expression []goqu.Expression, err error)
}

type ColumnIs []ColumnI

func (cols ColumnIs) SelectColumns() (columns []any) {
	columns = make([]any, 0)
	for _, col := range cols {
		columns = append(columns, col.SelectColumns()...)
	}
	return columns
}

func (cols ColumnIs) SelectWhere() (mergedWhere []goqu.Expression, err error) {
	wheres := make([]Where, 0)
	for _, col := range cols {
		wheres = append(wheres, WhereFn(col.SelectWhere))
	}
	mergedWhere, err = MergeWhere(wheres...)
	return mergedWhere, err
}

func (cols ColumnIs) InsertValue() (value any, err error) {
	valueIs := make([]Data, 0)
	for _, col := range cols {
		valueIs = append(valueIs, DataFn(col.InsertValue))
	}
	return MergeData(valueIs...)
}

func (cols ColumnIs) UpdateValue() (value any, err error) {
	valueIs := make([]Data, 0)
	for _, col := range cols {
		valueIs = append(valueIs, DataFn(col.UpdateValue))
	}
	return MergeData(valueIs...)
}

func (cols ColumnIs) UpdateWhere() (mergedWhere []goqu.Expression, err error) {
	wheres := make([]Where, 0)
	for _, col := range cols {
		wheres = append(wheres, WhereFn(col.UpdateWhere))
	}
	return MergeWhere(wheres...)
}

func (cols ColumnIs) Columns() ColumnIs { // 实现CommandI 接口部分函数
	return cols
}

type Column struct {
	Name               string          `json:"name"`
	InsertValueFn      DataFn          `json:"-"`
	SelectWhereValueFn DataFn          `json:"-"`
	UpdateWhereValueFn DataFn          `json:"-"`
	UpdateValueFn      DataFn          `json:"-"`
	Schema             ColumnSchema    `json:"schema"`          // 用于验证字段的规则约束
	BusinessSchemas    BusinessSchemas `json:"businessSchemas"` // 基于业务的验证规则
}

func (c Column) SelectColumns() (columns []any) {
	return []any{c.Name}
}
func (c Column) SelectWhere() (expression []goqu.Expression, err error) {
	if c.SelectWhereValueFn != nil {
		return nil, nil
	}
	val, err := c.SelectWhereValueFn()
	if err != nil {
		return nil, err
	}
	if ex, ok := TryParseExpressions(c.Name, val); ok {
		return ex, nil
	}
	return ConcatExpression(goqu.Ex{c.Name: val}), nil
}
func (c Column) InsertValue() (value any, err error) {
	if c.InsertValueFn == nil {
		return nil, nil
	}
	return c.InsertValueFn()
}
func (c Column) UpdateValue() (value any, err error) {
	if c.UpdateValueFn == nil {
		return nil, nil
	}
	return c.UpdateValueFn()
}
func (c Column) UpdateWhere() (expression []goqu.Expression, err error) {
	if c.UpdateWhereValueFn != nil {
		return nil, nil
	}
	val, err := c.UpdateWhereValueFn()
	if err != nil {
		return nil, err
	}
	if ex, ok := TryParseExpressions(c.Name, val); ok {
		return ex, nil
	}
	return ConcatExpression(goqu.Ex{c.Name: val}), nil
}

type CommandI interface {
	Columns() ColumnIs
	Table
}

type QueryI interface {
	CommandI
	_Pagination
	_Order
}

func Insert(commandI CommandI) (rawSql string, err error) {
	value, err := commandI.Columns().InsertValue()
	if err != nil {
		return "", err
	}
	ds := Dialect.Insert(commandI.Table()).Rows(value)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func Update(commandI CommandI) (rawSql string, err error) {
	value, err := commandI.Columns().UpdateValue()
	if err != nil {
		return "", err
	}
	where, err := commandI.Columns().UpdateWhere()
	if err != nil {
		return "", err
	}
	ds := Dialect.Update(commandI.Table()).Set(value).Where(where...)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func List(queryI QueryI) (rawSql string, err error) {
	where, err := queryI.Columns().SelectWhere()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := queryI.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}
	ds := Dialect.Select(queryI.Columns().SelectColumns()...).
		From(queryI.Table()).
		Where(where...).
		Order(queryI.Order()...)
	if pageSize > 0 {
		ds = ds.Offset(uint(ofsset)).Limit(uint(pageSize))
	}
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func First(queryI QueryI) (rawSql string, err error) {
	where, err := queryI.Columns().SelectWhere()
	if err != nil {
		return "", err
	}

	ds := Dialect.Select(queryI.Columns().SelectColumns()...).
		From(queryI.Table()).
		Where(where...).
		Order(queryI.Order()...).Limit(1)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func Total(commandI CommandI) (sql string, err error) {
	where, err := commandI.Columns().SelectWhere()
	if err != nil {
		return "", err
	}
	ds := Dialect.From(commandI.Table()).Where(where...).Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

func QuerySQL(sqlFn func(ds *goqu.SelectDataset) (newDs *goqu.SelectDataset, err error)) (sql string, err error) {
	ds := Dialect.Select()
	ds, err = sqlFn(ds)
	if err != nil {
		return "", err
	}
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

// 基于数据表填充对应数据，同时也可以基于此生成SQL DDL
type ColumnSchema struct {
	Title      string `json:"title"`
	Required   bool   `json:"required,string"` // 对应数据库的not null
	AllowEmpty bool   `json:"allowEmpty,string"`
	Comment    string `json:"comment"`
	Type       string `json:"type"`
	Default    any    `json:"default"`
	Enums      Enums  `json:"enums"`
}

type Enums []Enum

type Enum struct {
	Title string `json:"title"`
	Const string `json:"const"`
	Value string `json:"value"`
}

// Validator 返回验证器
// todo
func (c ColumnSchema) Validator() (err error) {
	return
}

type BusinessSchema struct {
	Name string `json:"name"`
	Expr string `json:"expr"`
}

type BusinessSchemas []BusinessSchema

type Model struct {
	QueryI
}

func (m Model) Insert() (rawSql string, err error) {
	return Insert(m.QueryI)
}
func (m Model) Update() (rawSql string, err error) {
	return Update(m.QueryI)
}
func (m Model) List() (rawSql string, err error) {
	return List(m.QueryI)
}

func (m Model) First() (rawSql string, err error) {
	return First(m.QueryI)
}
func (m Model) Total() (rawSql string, err error) {
	return Total(m.QueryI)
}

func (m Model) Query(sqlFn func(ds *goqu.SelectDataset) (newDs *goqu.SelectDataset, err error)) (rawSql string, err error) {
	return QuerySQL(sqlFn)
}
