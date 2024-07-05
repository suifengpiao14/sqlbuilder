package sqlbuilder

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

var Time_format = "2024-01-02 15:04:05"

type ValueFn func(in any) (value any, err error) // 函数之所有接收in 入参，有时模型内部加工生成的数据需要存储，需要定制格式化，比如多边形产生的边界框4个点坐标

type ValueFns []ValueFn

func (fns *ValueFns) Insert(index int, subFns ...ValueFn) {
	if *fns == nil {
		*fns = make(ValueFns, 0)
	}
	if index < 0 { // index 小于0,则直接追加,最好采用-1,后续可能细化负数
		*fns = append(*fns, subFns...)
	}
	if index == 0 { // index =0 插入第一个
		tmp := make(ValueFns, 0)
		tmp = append(tmp, subFns...)
		tmp = append(tmp, *fns...)
		*fns = tmp
		return
	}
	if len(*fns) < index { // 当前长度小于指定的开始索引,则不插入,通过这个方法能确保中间件修改函数不会插入到第一个
		return
	}
	pre, after := (*fns)[:index], (*fns)[index:]
	tmp := make(ValueFns, 0)
	tmp = append(tmp, pre...)
	tmp = append(tmp, subFns...)
	tmp = append(tmp, after...)
	*fns = tmp

}

// ValueFnDirect 原样返回
func ValueFnDirect(val any) (value any, err error) {
	return val, nil
}

// ValueFnFromData 从 field.Data获取数据
func ValueFnFromData(field Field) ValueFn {
	return func(in any) (value any, err error) {
		return field.Data()
	}
}

// ShieldFormat 屏蔽值，常用于取消某个字段作为查询条件
func ShieldFormat(val any) (value any, err error) {
	return nil, nil
}

// Field 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Field struct {
	Title     string                                                 `json:"title"`
	Name      string                                                 `json:"name"`
	ValueFns  ValueFns                                               `json:"-"` // 增加error，方便封装字段验证规则
	WhereFns  ValueFns                                               `json:"-"` // 当值作为where条件时，调用该字段格式化值，该字段为nil则不作为where条件查询,没有error，验证需要在ValueFn 中进行,数组支持中间件添加转换函数，转换函数在field.GetWhereValue 统一执行
	Migrate   func(table string, options ...MigrateOptionI) Migrates `json:"-"`
	Validator func(field Field) ValidateI                            `json:"-"` // 设置验证参数验证器
	DBSchema  *DBSchema                                              // 可以为空，为空建议设置默认值
}

// 给当前列增加where条件修改
func (f Field) AppendWhereFn(fns ...ValueFn) {
	if f.WhereFns == nil {
		f.WhereFns = make(ValueFns, 0)
	}
	addr := &f.WhereFns
	*addr = append(*addr, fns...)
}

// 给当前列增加value数据修改
func (f Field) AppendValueFn(fns ...ValueFn) {
	if f.ValueFns == nil {
		f.ValueFns = make(ValueFns, 0)
	}
	addr := &f.ValueFns
	*addr = append(*addr, fns...)
}

type MiddlewareI interface {
}

// LogString 日志字符串格式
func (f Field) LogString() string {
	title := f.Title
	if title == "" {
		title = f.Name
	}
	val, _ := f.GetValue(nil)
	str := cast.ToString(val)
	out := fmt.Sprintf("%s(%s)", title, str)
	return out
}

var ERROR_VALUE_NIL = errors.New("error value nil")

func IsErrorValueNil(err error) bool {
	return errors.Is(ERROR_VALUE_NIL, err)
}

func (f Field) GetValue(in any) (value any, err error) {
	value = in
	for _, fn := range f.ValueFns {
		value, err = fn(value) //格式化值
		if err != nil {
			return value, err
		}
	}
	if IsNil(value) {
		err = ERROR_VALUE_NIL //相比返回 nil,nil; 此处抛出错误，其它地方更容易感知中断处理，如需要继续执行，执行忽略这个类型Error 即可
		return nil, err
	}
	return value, nil
}

// GetWhereValue 获取Where 值
func (f Field) GetWhereValue(in any) (value any, err error) {
	if len(f.WhereFns) == 0 {
		return nil, nil
	}
	value = in
	value, err = f.GetValue(in)
	if err != nil {
		return value, err
	}

	for _, fn := range f.WhereFns {
		value, err = fn(value)
		if err != nil {
			return value, err
		}
	}
	if IsNil(value) {
		return nil, nil
	}

	return value, nil
}

// IsEqual 判断名称值是否相等
func (f Field) IsEqual(o Field) bool {
	fv, err := f.GetValue(nil)
	if err != nil || IsNil(fv) {
		return false
	}
	ov, err := o.GetValue(nil)
	if err != nil || IsNil(ov) {
		return false
	}
	return strings.EqualFold(cast.ToString(fv), cast.ToString(ov)) && strings.EqualFold(f.Name, o.Name)
}

// Validate  实现ValidateI 接口 可以再 valueFn ,whereValueFn 中手动调用
func (c Field) Validate(val any) (err error) {
	if c.DBSchema == nil {
		return nil
	}
	rv := reflect.Indirect(reflect.ValueOf(val))
	err = c.DBSchema.Validate(c.Name, rv)
	if err != nil {
		return err
	}

	return
}

func (f Field) Data() (data any, err error) {
	return f.GetValue(nil)
}

func (f Field) Where() (expressions Expressions, err error) {
	val, err := f.GetWhereValue(nil)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if ex, ok := TryParseExpressions(f.Name, val); ok {
		return ex, nil
	}
	return ConcatExpression(goqu.Ex{f.Name: val}), nil
}

type Fields []Field

func (fs Fields) Where() (expressions Expressions, err error) {
	expressions = make(Expressions, 0)
	for _, field := range fs {
		subExprs, err := field.Where()
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, subExprs...)
	}
	return expressions, nil
}

func (fs Fields) Json() string {
	b, _ := json.Marshal(fs)
	return string(b)
}
func (fs Fields) String() string {
	m := make(map[string]any)
	for _, f := range fs {
		val, _ := f.GetValue(nil)
		m[f.Name] = val
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func (fs Fields) Data() (data any, err error) {
	m := make(map[string]any)
	for _, f := range fs {
		val, err := f.GetValue(nil)
		if IsErrorValueNil(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		m[f.Name] = val
	}
	return m, nil
}

type FieldFn func() []Field

func (fn FieldFn) Data() (data any, err error) {
	m := map[string]any{}
	columns := fn()
	for _, c := range columns {
		if c.Name != "" {
			val, err := c.GetValue(nil)
			if IsErrorValueNil(err) {
				continue
			}

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

func TryIlike(field string, value any) (expressions Expressions, ok bool) {
	if iLike, ok := value.(Ilike); ok {
		identifier := goqu.C(field)
		strArr := make([]string, 0)
		for _, arg := range iLike {
			strArr = append(strArr, cast.ToString(arg))
		}
		val := strings.Join(strArr, "")
		return ConcatExpression(identifier.ILike(val)), true
	}
	return nil, false
}

// Between 介于2者之间(包含上下边界，对于不包含边界情况，可以修改值范围或者直接用表达式)
type Between [2]any

func TryConvert2Betwwen(field string, value any) (expressions Expressions, ok bool) {
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
func TryConvert2Expressions(value any) (expressions Expressions, ok bool) {
	if ex, ok := value.(Expressions); ok {
		return ex, true
	}
	if ex, ok := value.(goqu.Expression); ok {
		return ConcatExpression(ex), true
	}
	return nil, false
}

// TryParseExpressions 尝试解析where条件
func TryParseExpressions(field string, value any) (expressions Expressions, ok bool) {
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
