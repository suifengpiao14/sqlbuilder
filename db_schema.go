package sqlbuilder

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

// 基于数据表填充对应数据，同时也可以基于此生成SQL DDL
type Schema struct {
	Title     string     `json:"title"`
	Required  bool       `json:"required,string"` // 对应数据库的not null
	Comment   string     `json:"comment"`
	Type      SchemaType `json:"type"`
	Default   any        `json:"default"`
	Enums     Enums      `json:"enums"`
	MaxLength int        `json:"maxLength"` // 字符串最大长度
	MinLength int        `json:"minLength"` // 字符串最小长度
	Maximum   uint       `json:"maximum"`   // 数字最大值
	Minimum   int        `json:"minimum"`   // 数字最小值
	RegExp    string     `json:"regExp"`    //正则表达式

	Primary       bool `json:"primary"` //是否为主键
	Unique        bool `json:"unique"`  // 是否为唯一键
	AutoIncrement bool `json:"autoIncrement"`
	ShieldUpdate  bool `json:"shieldUpdate"` //屏蔽更新该字段,适合不可更新字段,如tenat,deleted_at
}

func (schema Schema) FullComment() string {
	if schema.Comment == "" {
		schema.Comment = schema.Title
	}
	if len(schema.Enums) == 0 {
		return schema.Comment
	}
	return fmt.Sprintf("%s%s", schema.Comment, schema.Enums.String())
}

// AllowEmpty 是否可以为空
func (schema Schema) AllowEmpty() bool {
	return schema.MinLength < 1 && schema.Type == Schema_Type_string
}

// SchemaType 重新声明类型后，使用时IDE能自动识别到常量定义
type SchemaType string

func (st SchemaType) String() string {
	return string(st)
}

func (st SchemaType) IsEqual(other SchemaType) bool {
	return strings.EqualFold(string(st), string(other))
}

const (
	Schema_Type_string SchemaType = "string"
	Schema_Type_json   SchemaType = "json"
	Schema_Type_int    SchemaType = "int"
)

type Enums []Enum

// append 排重后面再融入优化
func (es *Enums) append(enums ...Enum) {
	exists := false
	for _, en := range enums {
		for _, e := range *es {
			if e.IsEqual(en) {
				exists = true
				break
			}
		}
		if !exists {
			*es = append(*es, en)
		}
	}

}

// Insert 追加元素,不建议使用,建议用InsertAsFirst,InsertAsSecond
func (es *Enums) Insert(index int, enums ...Enum) {
	if *es == nil {
		*es = make(Enums, 0)
	}
	l := len(*es)
	if l == 0 || index < 0 || l <= index { // 本身没有,直接添加,或者计划添加到结尾,或者指定位置比现有数组长,直接追加
		*es = append(*es, enums...)
		return
	}
	if index == 0 { // index =0 插入第一个
		tmp := make(Enums, 0)
		tmp = append(tmp, enums...)
		tmp = append(tmp, *es...)
		*es = tmp
		return
	}
	pre, after := (*es)[:index], (*es)[index:]
	tmp := make(Enums, 0)
	tmp = append(tmp, pre...)
	tmp = append(tmp, enums...)
	tmp = append(tmp, after...)
	*es = tmp
}

// InsertAsFirst 作为第一个元素插入,一般用于将数据导入到whereFn 中
func (es *Enums) InsertAsFirst(enums ...Enum) {
	es.Insert(0, enums...)
}

// RemoveByTag 通过tag 移除部分枚举值，尽量少用(到目前没使用)
func (es *Enums) RemoveByTag(tags ...string) {
	tmp := Enums{}
	for _, e := range *es {
		exists := false
		for _, t := range tags {
			if strings.EqualFold(e.Tag, t) {
				exists = true
				break
			}
		}
		if !exists {
			tmp = append(tmp, e)
		}
	}
	*es = tmp
}

// Append 常规添加
func (es *Enums) Append(enums ...Enum) {
	es.Insert(-1, enums...)
}

func (es Enums) Values() (values []any) {
	values = make([]any, 0)
	for _, e := range es {
		values = append(values, e.Key)
	}
	return values
}
func (es Enums) Contains(val any) (ok bool) {
	for _, e := range es {
		if ok = e.IsEqual(val); ok {
			return true
		}

	}
	return false
}

// SubEnums 用于提取部分枚举值(比如状态机提取能转到B状态的源，方便输出提示)
func (es Enums) SubEnums(keys ...any) (subEnums Enums) {
	subEnums = make(Enums, 0)
	for _, e := range es {
		for _, key := range keys {
			if ok := e.IsEqual(key); ok {
				subEnums = append(subEnums, e)
				break
			}
		}

	}
	return subEnums
}

func (es Enums) ValuesStr() (valuesStr []string) {
	values := es.Values()
	valuesStr = make([]string, 0)
	for _, v := range values {
		valuesStr = append(valuesStr, cast.ToString(v))
	}
	return valuesStr
}
func (es Enums) Title(key any) string {
	return es.Get(key).Title
}

func (es Enums) Get(key any) (enum Enum) {
	for _, e := range es {
		if e.IsEqual(key) {
			return e
		}
	}
	return
}

func (es Enums) GetByTag(tag string) (enum Enum) {
	for _, e := range es {
		if strings.EqualFold(e.Tag, tag) {
			return e
		}
	}
	return
}

func (es Enums) Type() (typ string) {
	if len(es) == 0 {
		return Schema_Type_string.String()
	}
	e := es[0]
	if _, ok := e.Key.(int); ok {
		return Schema_Type_int.String()
	}
	return Schema_Type_string.String()
}

func (es Enums) Default() (enum Enum) {

	for _, e := range es {
		if e.IsDefault {
			return e
		}
	}
	return enum
}

func (es Enums) MaxLengthMaximum() (maxLength int, maximum uint) {
	typ := es.Type()
	switch typ {
	case Schema_Type_int.String():
		for _, e := range es {
			num := cast.ToUint(e.Key)
			if num > maximum {
				maximum = num
			}
		}
	case Schema_Type_string.String():
		for _, e := range es {
			length := len(cast.ToString(e.Key))
			if length > maxLength {
				maxLength = length
			}
		}
	}
	return maxLength << 1, maximum // 字符串长度扩大1倍
}

// String 生成文档有使用
func (es Enums) String() (str string) {
	values := make([]string, 0)
	for _, e := range es {
		values = append(values, fmt.Sprintf("%s-%s", cast.ToString(e.Key), e.Title))
	}
	return strings.Join(values, ",")
}

// NewEnumTitleField 根据enum field 生成title列
func NewEnumTitleField(key any, enumField *Field) *Field {
	valueFn := func(in any) (any, error) {
		return enumField.Schema.Enums.Title(key), nil
	}
	name := fmt.Sprintf("%sTitle", enumField.Name)
	title := fmt.Sprintf("%s标题", enumField.Schema.Title)
	f := NewField(valueFn).SetName(name).SetTitle(title)
	return f
}

type Enum struct {
	Key       any    `json:"key"`
	Title     string `json:"title"`
	Tag       string `json:"tag"`
	IsDefault bool   `json:"isDefault"`
}

const (
	Enum_tag_true  = "true"
	Enum_tag_false = "false"

	Enum_tag_allowEmpty = "allowEmpty" // 枚举值可以为空，通常用于选择条件
)

func (e Enum) IsEqual(val any) (ok bool) {
	ok = strings.EqualFold(cast.ToString(e.Key), cast.ToString(val))
	return ok
}

func (schema Schema) Validate(fieldName string, field reflect.Value) error {
	// 验证 required
	isNotValid := !field.IsValid() // 空值 由nil 过来的值
	if schema.Required && (isNotValid || isEmptyValue(field)) {
		return fmt.Errorf("%s is required", fieldName)
	}
	if isNotValid {
		return nil // 空值不判断，因为nil代表忽略这个字段
	}

	var valStr string
	var varInt int64

	kind := field.Kind()
	switch kind {
	case reflect.String:
		valStr = field.String()
	case reflect.Int:
		varInt = field.Int()
	}

	// 验证 maxLength
	if schema.MaxLength > 0 && kind == reflect.String && len(valStr) > schema.MaxLength {
		return fmt.Errorf("%s exceeds maximum length of %d", fieldName, schema.MaxLength)
	}
	// 验证 minLength
	if schema.MinLength > 0 && kind == reflect.String && len(valStr) < schema.MinLength {
		return fmt.Errorf("%s is less than minimum length of %d", fieldName, schema.MinLength)
	}
	// 验证 maximum
	if schema.Maximum > 0 && kind == reflect.Int && varInt > 0 && uint(varInt) > schema.Maximum {
		return fmt.Errorf("%s exceeds maximum value of %d", fieldName, schema.Maximum)
	}
	// 验证 minimum

	if schema.Minimum > 0 {
		if kind == reflect.Int && varInt < int64(schema.Minimum) {
			return fmt.Errorf("%s is less than minimum value of %d", fieldName, schema.Minimum)
		}
		if schema.Type == Schema_Type_int {
			varInt = cast.ToInt64(field.Interface())
			if varInt < int64(schema.Minimum) {
				return fmt.Errorf("%s as number is less than minimum value of %d", fieldName, schema.Minimum)
			}
		}

	}
	// 验证 enums
	if len(schema.Enums) > 0 {
		val := field.Interface()
		if !contains(schema.Enums.ValuesStr(), cast.ToString(val)) {
			return fmt.Errorf("%s must be one of %v,got:%v", fieldName, schema.Enums.Values(), val)
		}
	}
	if schema.RegExp != "" {
		ex, err := regexp.Compile(schema.RegExp)
		if err != nil {
			err = errors.WithMessagef(err, "Schema.Validate,field name:%s,regExp:%s", fieldName, schema.RegExp)
			return err
		}
		str := cast.ToString(field.Interface())
		if !ex.MatchString(str) {
			err = errors.Errorf("%s RegExp is %s,got:%s", fieldName, schema.RegExp, str)
			return err
		}
	}
	return nil
}

func isEmptyValue(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	// vStr := cast.ToString(v.Interface())
	// return vStr == ""

	switch v.Kind() {
	case reflect.String:
		return v.Len() == 0
	case reflect.Array, reflect.Slice, reflect.Map, reflect.Chan:
		return v.Len() == 0
	// case reflect.Bool:
	// 	return !v.Bool()
	// case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
	// 	return v.Int() == 0
	// case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
	// 	return v.Uint() == 0
	// case reflect.Float32, reflect.Float64:
	// 	return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false

}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

var ApplyValueFormatBySchemaType InitFieldFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(func(inputValue any) (any, error) {
		value := f.FormatType(inputValue)
		return value, nil
	})
}

// ValueFnDBSchemaFormat 根据DB类型要求,转换数据类型
func ValueFnDBSchemaFormatType(field Field) (valueFn ValueFn) {
	return func(in any) (any, error) {
		value := field.FormatType(in)
		return value, nil
	}
}

// ValueFnUniqueueArray 数组元素去重
func ValueFnUniqueueArray[T int | int64 | string](in any) (any, error) {
	arr, ok := in.([]T)
	if !ok {
		return in, nil
	}
	newArr := make([]T, 0)
	m := map[T]struct{}{}
	for _, elem := range arr {
		if _, ok := m[elem]; ok {
			continue
		}
		m[elem] = struct{}{}
		newArr = append(newArr, elem)
	}
	return newArr, nil

}

func ValueFnForward(in any) (any, error) {
	return in, nil
}

// ValueFnFormatArray 格式化数组,只有一个元素时,直接返回当前元素，常用户where in 条件
func ValueFnFormatArray(in any) (any, error) {
	valValue := reflect.Indirect(reflect.ValueOf(in))
	valType := valValue.Type()
	switch valType.Kind() {
	case reflect.Slice, reflect.Array:
		if valValue.Len() == 1 {
			in = valValue.Index(0).Interface()
		}
	}
	return in, nil
}

// ValueFnDecodeComma 参数中,拼接的字符串解码成数组
func ValueFnDecodeComma(in any) (any, error) {
	if IsNil(in) {
		return in, nil
	}
	s := cast.ToString(in)
	if !strings.Contains(s, ",") {
		return in, nil
	}
	arr := strings.Split(s, ",")
	uniqArr, _ := ValueFnUniqueueArray[string](arr) // 去重
	strArr := uniqArr.([]string)
	if len(strArr) == 1 {
		in = strArr[0] // 去重后只有一个，则转成字符串
	} else {
		in = strArr
	}
	return in, nil
}

func ValueFnShield(in any) (any, error) {
	return nil, nil
}

func ValueFnEmpty2Nil(in any) (any, error) {
	switch val := in.(type) {
	case string:
		if val == "" {
			return nil, nil
		}
	case int:
		if val == 0 {
			return nil, nil
		}
	}
	return in, nil
}
func ValueFnIlike(in any) (value any, err error) {
	if IsNil(in) {
		return nil, nil
	}
	return Ilike{"%", cast.ToString(in), "%"}, nil
}

var ApplyWhereGte InitFieldFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(func(inputValue any) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		ex := goqu.C(f.DBName()).Gte(inputValue)
		return ex, nil
	})
}

var ApplyWhereLte InitFieldFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(func(inputValue any) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		ex := goqu.C(f.DBName()).Lte(inputValue)
		return ex, nil
	})
}

// var ApplyWhereFindInSet InitFieldFn = func(f *Field, fs ...*Field) {
// 	f.WhereFns.Append(func(inputValue any) (any, error) {
// 		if IsNil(inputValue) {
// 			return nil, nil
// 		}
// 		expression := goqu.L("FIND_IN_SET(?,?)", cast.ToString(inputValue), goqu.C(f.DBName()))
// 		return expression, nil
// 	})
// }

// GlobalValueFnEmptyStr2Nil 空字符串改成nil,值改成nil后,sql语句中会忽略该字段,常常用在update,where 字句中
// func GlobalValueFnEmptyStr2Nil(field Field, exceptFileds ...*Field) (valueFn ValueFn) {
// 	return func(in any) (any, error) {
// 		if Fields(exceptFileds).Contains(field) {
// 			return in, nil
// 		}
// 		str := cast.ToString(in)
// 		if str == "" {
// 			return nil, nil
// 		}
// 		return in, nil
// 	}
// }
