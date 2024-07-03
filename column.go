package sqlbuilder

import (
	"encoding/json"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
)

type ColumnI interface {
	SelectColumn() (column any)
	SelectWhere() (expression []goqu.Expression, err error)
	InsertData() (data any, err error)
	UpdateData() (data any, err error)
	UpdateWhere() (expression []goqu.Expression, err error)
}

type ColumnIs []ColumnI

func (cols ColumnIs) SelectColumn() (column any) { // 此处之所以返回any 不是[]any 是方便 ColumnIs 实现 ColumnI 接口
	columns := make([]any, 0)
	for _, col := range cols {
		columns = append(columns, col.SelectColumn())
	}
	return columns
}

func (cols ColumnIs) SelectWhere() (mergedWhere []goqu.Expression, err error) {
	wheres := make([]Where, 0)
	for _, col := range cols {
		wheres = append(wheres, WhereFn(col.SelectWhere))
	}
	return MergeWhere(wheres...)
}

func (cols ColumnIs) InsertData() (data any, err error) {
	dataIs := make([]Data, 0)
	for _, col := range cols {
		dataIs = append(dataIs, DataFn(col.InsertData))
	}
	return MergeData(dataIs...)
}

func (cols ColumnIs) UpdateData() (data any, err error) {
	dataIs := make([]Data, 0)
	for _, col := range cols {
		dataIs = append(dataIs, DataFn(col.UpdateData))
	}
	return MergeData(dataIs...)
}

func (cols ColumnIs) UpdateWhere() (mergedWhere []goqu.Expression, err error) {
	wheres := make([]Where, 0)
	for _, col := range cols {
		wheres = append(wheres, WhereFn(col.UpdateWhere))
	}
	return MergeWhere(wheres...)
}

type TableI interface {
	Columns() ColumnIs
	Table
	_Pagination
	_Order
}

func Insert(tableI TableI) (rawSql string, err error) {
	data, err := tableI.Columns().InsertData()
	if err != nil {
		return "", err
	}
	ds := Dialect.Insert(tableI.Table()).Rows(data)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func Update(tableI TableI) (rawSql string, err error) {
	data, err := tableI.Columns().UpdateData()
	if err != nil {
		return "", err
	}
	where, err := tableI.Columns().UpdateWhere()
	if err != nil {
		return "", err
	}
	ds := Dialect.Update(tableI.Table()).Set(data).Where(where...)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func List(tableI TableI) (rawSql string, err error) {
	where, err := tableI.Columns().SelectWhere()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := tableI.Pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}
	columns, ok := (tableI.Columns().SelectColumn()).([]any) // 此处类型一定成功，是内部函数返回
	if !ok {
		err = errors.Errorf("tableI.Columns().SelectColumn() return []any")
		panic(err)
	}

	ds := Dialect.Select(columns...).
		From(tableI.Table()).
		Where(where...).
		Order(tableI.Order()...)
	if pageSize > 0 {
		ds = ds.Offset(uint(ofsset)).Limit(uint(pageSize))
	}
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func First(tableI TableI) (rawSql string, err error) {
	where, err := tableI.Columns().SelectWhere()
	if err != nil {
		return "", err
	}

	columns, ok := (tableI.Columns().SelectColumn()).([]any) // 此处类型一定成功，是内部函数返回
	if !ok {
		err = errors.Errorf("tableI.Columns().SelectColumn() return []any")
		panic(err)
	}

	ds := Dialect.Select(columns...).
		From(tableI.Table()).
		Where(where...).
		Order(tableI.Order()...).Limit(1)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func Total(tableI TableI) (sql string, err error) {
	where, err := tableI.Columns().SelectWhere()
	if err != nil {
		return "", err
	}
	ds := Dialect.From(tableI.Table()).Where(where...).Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

type Column struct {
	Name        string
	InsertData  DataFn
	SelectWhere WhereFn
	UpdateWhere WhereFn
	UpdateData  DataFn
	Schema      ColumnSchema // 用于验证字段的规则约束
}

// Multicolumn 复合列
type Multicolumn struct {
	Columns Columns
}

type ColumnSchema struct {
	Title       string `json:"title"`
	Required    bool   `json:"required,string"`
	AllowEmpty  bool   `json:"allowEmpty,string"`
	Description string `json:"description"`
	Type        string `json:"type"`
}

// Validator 返回验证器
// todo
func (c ColumnSchema) Validator() {
	return
}

type SQLBuilder struct {
	Select goqu.SelectDataset
}

type Columns []Column

func (fs Columns) Insert(table string) (rawSql string, err error) {
	rowData, err := fs.MapInsertData()
	if err != nil {
		return rawSql, err
	}
	ds := Dialect.Insert(table).Rows(rowData)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func (fs Columns) Update(table string) (rawSql string, err error) {
	data, err := fs.MapUpdateData()
	if err != nil {
		return "", err
	}
	where, err := fs._SelectWhere()
	if err != nil {
		return "", err
	}
	ds := Dialect.Update(table).Set(data).Where(where...)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func (fs Columns) First(table string, orderedExpressions []exp.OrderedExpression) (rawSql string, err error) {
	where, err := fs._SelectWhere()
	if err != nil {
		return "", err
	}
	ds := Dialect.Select(fs._Column()...).
		From(table).
		Where(where...).
		Order(orderedExpressions...).
		Limit(1)
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func (fs Columns) List(table string, pagination PaginationFn, orderedExpressions []exp.OrderedExpression) (rawSql string, err error) {
	where, err := fs._SelectWhere()
	if err != nil {
		return "", err
	}
	pageIndex, pageSize := pagination()
	ofsset := pageIndex * pageSize
	if ofsset < 0 {
		ofsset = 0
	}

	ds := Dialect.Select(fs._Column()...).
		From(table).
		Where(where...).
		Order(orderedExpressions...)
	if pageSize > 0 {
		ds = ds.Offset(uint(ofsset)).Limit(uint(pageSize))
	}
	rawSql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return rawSql, nil
}

func (fs Columns) Total(table string) (sql string, err error) {
	where, err := fs._SelectWhere()
	if err != nil {
		return "", err
	}
	ds := Dialect.From(table).Where(where...).Select(goqu.COUNT(goqu.Star()).As("count"))
	sql, _, err = ds.ToSQL()
	if err != nil {
		return "", err
	}
	return sql, nil
}

func (fs Columns) _SelectWhere() (where []goqu.Expression, err error) {
	where = make([]exp.Expression, 0)
	for _, f := range fs {
		ex, err := f.SelectWhere()
		if err != nil {
			return where, err
		}
		if IsNil(ex) {
			continue
		}
		where = append(where, ex...)
	}
	return where, nil
}

func (fs Columns) _Column() (columns []any) {
	columns = make([]any, 0)
	for _, f := range fs {
		key := f.Name
		if key != "" {
			columns = append(columns, key)
		}
	}
	return columns
}

func (fs Columns) Json() string {
	b, _ := json.Marshal(fs)
	return string(b)
}
func (fs Columns) String() string {
	m := make(map[string]any)
	for _, f := range fs {
		val, _ := f.InsertData()
		m[f.Name] = val
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func (fs Columns) MapInsertData() (data map[string]any, err error) {
	m := make(map[string]any)
	for _, f := range fs {
		if f.InsertData == nil {
			continue
		}
		val, err := f.InsertData()
		if err != nil {
			return nil, err
		}
		m[f.Name] = val
	}
	return m, nil
}
func (fs Columns) MapUpdateData() (data map[string]any, err error) {
	m := make(map[string]any)
	for _, f := range fs {
		if f.UpdateData == nil {
			continue
		}
		val, err := f.UpdateData()
		if err != nil {
			return nil, err
		}
		m[f.Name] = val
	}
	return m, nil
}
