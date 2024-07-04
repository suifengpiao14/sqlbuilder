package sqlbuilder

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/spf13/cast"
)

var Time_format = "2024-01-02 15:04:05"

type ValueFnInfo struct {
	IsFnNil    bool // valueFn 函数是否为nil
	IsValueNil bool // valueFn() 返回是否为nil
}
type WhereValueFnInfo struct {
	IsFnNil             bool // valueFn 函数是否为nil
	IsWhereValueNil     bool // valueFn() 返回是否为nil
	IsDefaultExpression bool // 是否使用了默认表达式
}

// Field 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Field struct {
	Title             string                                                 `json:"title"`
	Name              string                                                 `json:"name"`
	ValueFn           func(in any) (value any, err error)                    `json:"-"` // 增加error，方便封装字段验证规则
	WhereValueFn      func(in any) (value any, err error)                    `json:"-"` // 值value 和where value分开
	Migrate           func(table string, options ...MigrateOptionI) Migrates `json:"-"`
	_ValueFnInfo      ValueFnInfo                                            // 方便继承类判断具体情况
	_WhereValueFnInfo WhereValueFnInfo                                       // 方便继承类判断具体情况
}

func (f Field) GetValueFnInfo() ValueFnInfo {
	return f._ValueFnInfo
}
func (f Field) GetWhereValueFnInfo() WhereValueFnInfo {
	return f._WhereValueFnInfo
}

// Field 实现 InsertParamI,UpdateParamI,ListParamI,FirstParamI,TotalParamI 几个接口
func (f Field) Table() string {
	return ""
}
func (f Field) Data() (data any, err error) {
	m := map[string]any{}
	if f.ValueFn == nil {
		f._ValueFnInfo.IsFnNil = true
		return nil, nil
	}
	val, err := f.ValueFn(nil)
	if err != nil {
		return nil, err
	}
	if IsNil(val) { // 返回值为nil，忽略字段
		f._ValueFnInfo.IsValueNil = true
		return nil, nil
	}
	m[f.Name] = val
	return m, nil
}

func (f Field) Where() (expressions []goqu.Expression, err error) {
	if f.WhereValueFn == nil {
		f._WhereValueFnInfo.IsFnNil = true
		return nil, nil
	}
	val, err := f.WhereValueFn(nil)
	if err != nil {
		return nil, err
	}
	if IsNil(val) {
		f._WhereValueFnInfo.IsWhereValueNil = true
		return nil, nil
	}
	if ex, ok := TryParseExpressions(f.Name, val); ok {
		return ex, nil
	}
	f._WhereValueFnInfo.IsDefaultExpression = true
	return ConcatExpression(goqu.Ex{f.Name: val}), nil
}

func (f Field) Select() (columns []any) {
	return []any{f.Name}
}
func (f Field) Order() (orderedExpressions []exp.OrderedExpression) {
	return
}

func (f Field) Pagination() (index int, size int) {
	return
}

type Fields []Field

func (fs Fields) Table() string {
	tables := make([]TableI, 0)
	for _, f := range fs {
		tables = append(tables, f)
	}
	var table any
	allTable := MergeTable(tables...)
	for _, t := range allTable { // 取第一个
		table = t
		break
	}
	return cast.ToString(table)
}

func (fs Fields) Data() (data any, err error) {
	dataIs := make([]DataI, 0)
	for _, f := range fs {
		dataIs = append(dataIs, f)
	}
	return MergeData(dataIs...)
}

func (fs Fields) Where() (expressions []goqu.Expression, err error) {
	whereIs := make([]WhereI, 0)
	for _, f := range fs {
		whereIs = append(whereIs, f)
	}
	return MergeWhere(whereIs...)
}

func (fs Fields) Select() (columns []any) {
	selectIs := make([]SelectI, 0)
	for _, f := range fs {
		selectIs = append(selectIs, f)
	}
	return MergeSelect(selectIs...)
}

func (fs Fields) Order() (orderedExpressions []exp.OrderedExpression) {
	orderIs := make([]OrderI, 0)
	for _, f := range fs {
		orderIs = append(orderIs, f)
	}
	return MergeOrder(orderIs...)
}

func (fs Fields) Json() string {
	b, _ := json.Marshal(fs)
	return string(b)
}
func (fs Fields) String() string {
	m := make(map[string]any)
	for _, f := range fs {
		val, _ := f.ValueFn(nil)
		m[f.Name] = val
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func (fs Fields) Pagination() (index int, size int) {
	paginationIs := make([]PaginationI, 0)
	for _, f := range fs {
		paginationIs = append(paginationIs, f)
	}
	return MergePagination(paginationIs...)
}

func (fs Fields) Map() (data map[string]any, err error) {
	m := make(map[string]any)
	for _, f := range fs {
		if f.ValueFn == nil {
			continue
		}
		val, err := f.ValueFn(nil)
		if err != nil {
			return nil, err
		}
		m[f.Name] = val
	}
	return m, nil
}

func NewField(name string, value func(in any) (any, error)) Field {
	if value == nil {
		value = func(in any) (any, error) {
			return in, nil
		}
	}
	column := Field{
		Name:    name,
		ValueFn: value,
	}
	return column
}

type FieldFn func() []Field

func (fn FieldFn) Data() (data any, err error) {
	m := map[string]any{}
	columns := fn()
	for _, c := range columns {
		if c.Name != "" {
			val, err := c.ValueFn(nil)
			if err == nil {
				return nil, err
			}
			m[c.Name] = val
		}
	}
	return m, nil
}

func IsNil(v any) bool {
	if v == nil {
		return true
	}
	valueOf := reflect.ValueOf(v)
	k := valueOf.Kind()
	switch k {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return valueOf.IsNil()
	default:
		return v == nil
	}
}

// Ilike 不区分大小写like语句
type Ilike [3]any

func TryIlike(field string, value any) (expressions []goqu.Expression, ok bool) {
	if iLike, ok := value.(Ilike); ok {
		identifier := goqu.C(field)
		strArr := make([]string, 0)
		for _, arg := range iLike {
			strArr = append(strArr, cast.ToString(arg))
		}
		val := strings.Join(strArr, ",")
		return ConcatExpression(identifier.ILike(val)), true
	}
	return nil, false
}

// Between 介于2者之间(包含上下边界，对于不包含边界情况，可以修改值范围或者直接用表达式)
type Between [2]any

func TryConvert2Betwwen(field string, value any) (expressions []goqu.Expression, ok bool) {
	if between, ok := value.(Between); ok {
		identifier := goqu.C(field)
		min, max := between[0], between[1]
		if !IsNil(min) && !IsNil(max) {
			expressions = append(expressions, identifier.Between(exp.NewRangeVal(min, max)))
			return expressions, true
		}
		if !IsNil(min) {
			return ConcatExpression(identifier.Gte(min)), true
		}

		if !IsNil(max) {
			return ConcatExpression(identifier.Lte(min)), true
		}
	}
	return nil, false
}

// TryConvert2Expressions 业务where 条件判断，优先判断是否符可以转换为条件，可以直接应用
func TryConvert2Expressions(value any) (expressions []goqu.Expression, ok bool) {
	if ex, ok := value.([]goqu.Expression); ok {
		return ex, true
	}
	if ex, ok := value.(goqu.Expression); ok {
		return ConcatExpression(ex), true
	}
	return nil, false
}

// TryParseExpressions 尝试解析where条件
func TryParseExpressions(field string, value any) (expressions []goqu.Expression, ok bool) {
	if ex, ok := TryConvert2Expressions(value); ok {
		return ex, true
	}
	if ex, ok := TryConvert2Betwwen(field, value); ok {
		return ex, true
	}

	if ex, ok := TryIlike(field, value); ok {
		return ex, true
	}
	return nil, false
}
func Expression2String(expressions ...goqu.Expression) string {
	sql, _, _ := Dialect.Select().Where(expressions...).ToSQL()
	return sql
}
