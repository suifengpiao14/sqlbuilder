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

// type ValueFnfunc(in any) (value any,err error) //函数签名返回参数命名后,容易误导写成 func(in any) (value any,err error){return value,nil};  正确代码:func(in any) (value any,err error){return in,nil};
type ValueFn func(in any) (any, error) // 函数之所有接收in 入参，有时模型内部加工生成的数据需要存储，需要定制格式化，比如多边形产生的边界框4个点坐标

type ValueFns []ValueFn

// Insert 追加元素,不建议使用,建议用InsertAsFirst,InsertAsSecond
func (fns *ValueFns) Insert(index int, subFns ...ValueFn) {
	if *fns == nil {
		*fns = make(ValueFns, 0)
	}
	l := len(*fns)
	if l == 0 || index < 0 || l <= index { // 本身没有,直接添加,或者计划添加到结尾,或者指定位置比现有数组长,直接追加
		*fns = append(*fns, subFns...)
		return
	}
	if index == 0 { // index =0 插入第一个
		tmp := make(ValueFns, 0)
		tmp = append(tmp, subFns...)
		tmp = append(tmp, *fns...)
		*fns = tmp
		return
	}
	pre, after := (*fns)[:index], (*fns)[index:]
	tmp := make(ValueFns, 0)
	tmp = append(tmp, pre...)
	tmp = append(tmp, subFns...)
	tmp = append(tmp, after...)
	*fns = tmp
}

// InsertAsFirst 作为第一个元素插入,一般用于将数据导入到whereFn 中
func (fns *ValueFns) InsertAsFirst(subFns ...ValueFn) {
	fns.Insert(0, subFns...)
}

// InsertAsSecond 作为第二个元素插入,一般用于在获取数据后立即验证器插入
func (fns *ValueFns) InsertAsSecond(subFns ...ValueFn) {
	fns.Insert(1, subFns...)
}

// Append 常规添加
func (fns *ValueFns) Append(subFns ...ValueFn) {
	fns.Insert(-1, subFns...)
}

// AppendIfNotFirst 追加到最后,但是不能是第一个,一般用于生成SQL时格式化数据
func (fns *ValueFns) AppendIfNotFirst(subFns ...ValueFn) {
	if len(*fns) == 0 {
		return
	}
	fns.Append(subFns...)
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
func NewValueFns(fn func() (value any, err error)) ValueFns {
	return ValueFns{
		func(in any) (value any, err error) {
			return fn()
		},
	}
}

// ShieldFormat 屏蔽值，常用于取消某个字段作为查询条件
func ShieldFormat(val any) (value any, err error) {
	return nil, nil
}

// Field 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Field struct {
	Name      string                                                 `json:"name"`
	ValueFns  ValueFns                                               `json:"-"` // 增加error，方便封装字段验证规则
	WhereFns  ValueFns                                               `json:"-"` // 当值作为where条件时，调用该字段格式化值，该字段为nil则不作为where条件查询,没有error，验证需要在ValueFn 中进行,数组支持中间件添加转换函数，转换函数在field.GetWhereValue 统一执行
	Migrate   func(table string, options ...MigrateOptionI) Migrates `json:"-"`
	Validator func(field Field) ValidateI                            `json:"-"` // 设置验证参数验证器
	DBSchema  *DBSchema                                              // 可以为空，为空建议设置默认值
	Table     Table                                                  // 关联表,方便收集Table全量信息
	Api       interface{}                                            // 关联Api对象,方便收集Api全量信息
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
	title := f.Name
	if f.DBSchema != nil && f.DBSchema.Title == "" {
		title = f.DBSchema.Title
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

// ValueFnArgEmptyStr2NilExceptFields 将空字符串值转换为nil值时排除的字段,常见的有 deleted_at 字段,空置代表正常
var ValueFnArgEmptyStr2NilExceptFields = Fields{}

var GlobalFnValueFns = func(f Field) ValueFns {
	return ValueFns{
		ValueFnEmptyStr2Nil(f, ValueFnArgEmptyStr2NilExceptFields...), // 将空置转换为nil,代替对数据判断 if v==""{//ignore}
		ValueFnDBSchemaFormatType(f),                                  // 在转换为SQL前,将所有数据类型按照DB类型转换,主要是格式化int和string,提升SQL性能
		//todo 统一实现数据库字段前缀处理
		//todo 统一实现代码字段驼峰形转数据库字段蛇形
		//todo 统一实现数据库字段替换,方便数据库字段更名
		//todo 统一实现数据库字段屏蔽,方便废弃数据库字段
		//todo 虽然单次只有一个字段信息,但是所有SQL语句的字段都一定经过该节点,这就能收集到全量信息,进一步拓展其用途如(发布事件,其它订阅):
		//todo 1. 统一收集数据库字段名形成数据字典
		//todo 2. 统一收集api字段生成文档
		//todo ...
	}
}

func (f Field) GetValue(in any) (value any, err error) {
	value = in
	f.ValueFns.AppendIfNotFirst(GlobalFnValueFns(f)...) // 在最后生成SQL数据时追加格式化数据
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

func (c Field) FormatType(val any) (value any) {
	value = val
	if c.DBSchema == nil {
		return value
	}
	switch c.DBSchema.Type {
	case DBSchema_Type_string, DBSchema_Type_email, DBSchema_Type_phone:
		value = cast.ToString(value)
	case DBSchema_Type_int:
		value = cast.ToInt(value)
	}

	return value
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

func (fs Fields) Contains(field Field) (exists bool) {
	for _, f := range fs {
		if strings.EqualFold(f.Name, field.Name) { // 暂时值判断名称,后续根据需求,再增加类型
			return true
		}
	}
	return false
}

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

// GlobalFnFormatFieldName 全局函数钩子,统一修改字段列名称,比如统一增加列前缀F
var GlobalFnFormatFieldName = func(filedName string) string {
	return filedName
}

// GlobalFnFormatTableName 全局函数钩子,统一修改表名称,比如统一增加表前缀t_
var GlobalFnFormatTableName = func(tableName string) string {
	return tableName
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
