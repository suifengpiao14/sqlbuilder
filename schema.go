package sqlbuilder

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

// 基于数据表填充对应数据，同时也可以基于此生成SQL DDL
type Schema struct {
	Title     string     `json:"title"`
	Required  bool       `json:"required,string"` // 对应数据库的not null
	Comment   string     `json:"comment"`
	Type      SchemaType `json:"type"`
	Format    string     `json:"format"` // db 中时间字段，有时db为string,需要标记为时间格式，所以增加这个字段
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
	ShieldUpdate  bool `json:"shieldUpdate"` //这个地方还是保留，虽然可以结合场景和 sqlbuilder.ValueFnShieldForWrite 代替 但是比较麻烦，在set场景时，inert需要写值，update时不需要，所以需要这个字段
	ZeroAsEmpty   bool `json:"zeroAsEmpty"`  //0值是否当做空值处理，验证required 时有使用
}

const (
	Schema_format_date     = "date"
	Schema_format_time     = "time"
	Schema_format_dateTime = "datetime"
)

func (schema *Schema) Copy() *Schema {
	cp := *schema
	cp.Enums = make(Enums, 0)
	cp.Enums = append(cp.Enums, schema.Enums...) // 这里需要重新赋值，才能copy

	return &cp
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

func (schema Schema) SetMaxLength(max int) *Schema {
	schema.MaxLength = max
	return &schema
}
func (schema Schema) SetMaximum(max uint) *Schema {
	schema.Maximum = max
	return &schema
}

func (schema Schema) SetMinLength(min int) *Schema {
	schema.MinLength = min
	return &schema
}
func (schema Schema) SetMinimum(min int) *Schema {
	schema.Minimum = min
	return &schema
}
func (schema Schema) SetZeroAsEmpty(zeroAsEmpty bool) *Schema {
	schema.ZeroAsEmpty = true
	return &schema
}

func (schema Schema) SetDefault(def any) *Schema {
	schema.Default = def
	return &schema
}

func (schema Schema) SetRegExp(reg string) *Schema {
	schema.RegExp = reg
	return &schema
}

// AllowEmpty 是否可以为空
func (schema Schema) AllowEmpty() bool {
	return schema.MinLength < 1 && schema.Type == Schema_Type_string
}
func (s *Schema) Merge(megred Schema) *Schema {
	if megred.Title != "" {
		s.Title = megred.Title
	}
	if megred.Required {
		s.Required = megred.Required
	}

	if megred.Comment != "" {
		s.Comment = megred.Comment
	}
	if megred.Type != "" {
		s.Type = megred.Type
	}
	if megred.Format != "" {
		s.Format = megred.Format
	}
	if megred.Default != "" {
		s.Default = megred.Default
	}

	if len(megred.Enums) > 0 {
		s.Enums.Append(megred.Enums...)
	}

	if megred.MaxLength > 0 {
		s.MaxLength = megred.MaxLength
	}

	if megred.MinLength > 0 {
		s.MinLength = megred.MinLength
	}

	if megred.Maximum > 0 {
		s.Maximum = megred.Maximum
	}

	if megred.Minimum > 0 {
		s.Minimum = megred.Minimum
	}

	if megred.RegExp != "" {
		s.RegExp = megred.RegExp
	}

	if megred.Primary {
		s.Primary = megred.Primary
	}

	if megred.AutoIncrement {
		s.AutoIncrement = megred.AutoIncrement
	}
	if megred.Unique {
		s.Unique = megred.Unique
	}
	if megred.ShieldUpdate {
		s.ShieldUpdate = megred.ShieldUpdate
	}
	return s
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
	Schema_Type_int    SchemaType = "int"

	Schema_doc_Type_json   SchemaType = "json"   // 输出文档类型使用
	Schema_doc_Type_null   SchemaType = "null"   //  输出文档类型使用
	Schema_doc_Type_array  SchemaType = "array"  //  输出文档类型使用
	Schema_doc_Type_object SchemaType = "object" //  输出文档类型使用
)

const (
	UnsinedInt_maximum_tinyint   = 1<<8 - 1
	UnsinedInt_maximum_smallint  = 1<<16 - 1
	UnsinedInt_maximum_mediumint = 1<<24 - 1
	UnsinedInt_maximum_int       = 1<<32 - 1
	UnsinedInt_maximum_bigint    = 1<<64 - 1

	Int_maximum_tinyint   = 1<<7 - 1
	Int_maximum_smallint  = 1<<15 - 1
	Int_maximum_mediumint = 1<<23 - 1
	Int_maximum_int       = 1<<31 - 1
	Int_maximum_bigint    = 1<<63 - 1
)

type Enums []Enum

func (a Enums) Len() int           { return len(a) }
func (a Enums) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Enums) Less(i, j int) bool { return a[i].OrderDesc > a[j].OrderDesc }

func (es *Enums) Sort() *Enums {
	sort.Sort(es)
	return es
}

// append 排重后面再融入优化
func (es *Enums) Append(enums ...Enum) {
	exists := false
	for _, en := range enums {
		for _, e := range *es {
			if e.IsEqual(en.Key) {
				exists = true //存在忽略
				break
			}
		}
		if !exists {
			*es = append(*es, en)
		}
	}
}
func (es *Enums) Replace(enums ...Enum) {
	exists := false
	for _, en := range enums {
		for i := 0; i < len(*es); i++ {
			e := (*es)[i]
			if e.IsEqual(en) {
				exists = true
				en.OrderDesc = e.OrderDesc // 保存原来顺序
				(*es)[i] = en              //存在替换
				break
			}
		}
		if !exists {
			*es = append(*es, en)
		}
	}
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

func (es Enums) Validate(val any) error {
	rv := reflect.Indirect(reflect.ValueOf(val))
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			valStr := cast.ToString(rv.Index(i).Interface())
			if err := enumValidate(es, valStr); err != nil {
				err = errors.WithMessagef(err, "original value:%v", val)
				return err
			}
		}
	default:
		valStr := cast.ToString(val)
		return enumValidate(es, valStr)
	}

	return nil
}

func enumValidate(es Enums, enumStr string) error {
	if !contains(es.ValuesStr(), enumStr) {
		return fmt.Errorf("must be one of %v,got:%v", es.Values(), enumStr)
	}
	return nil
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

// ConvertEnumKeyType 转换枚举值类型
func ConvertEnumKeyType[T any](enumKeys ...T) (enumKeysAny []any) {
	enumKeysAny = make([]any, 0)

	for _, key := range enumKeys {
		enumKeysAny = append(enumKeysAny, key)

	}
	return enumKeysAny
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
	valueFn := ValueFn{
		Fn: func(in any, f *Field, fs ...*Field) (any, error) {
			return enumField.Schema.Enums.Title(key), nil
		},
		Layer: Value_Layer_SetValue,
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
	OrderDesc int    `json:"order"` // 排序 数字越大，越靠前
}

const (
	Enum_tag_true  = "true"
	Enum_tag_false = "false"

	Enum_tag_allowEmpty = "allowEmpty" // 枚举值可以为空，通常用于选择条件
)

func (e Enum) IsEqual(val any) (ok bool) {
	switch v := val.(type) {
	case int:
		ok = cast.ToInt(e.Key) == v
		return ok
	case string:
		ok = strings.EqualFold(cast.ToString(e.Key), v)
	default:
		ok = strings.EqualFold(cast.ToString(e.Key), cast.ToString(e.Key))
	}
	return ok
}

func (schema Schema) Validate(fieldName string, field reflect.Value) error {
	kind := field.Kind()
	switch kind {
	case reflect.Slice, reflect.Array:
		//循环验证数组
		for i := 0; i < field.Len(); i++ {
			err := schema.validate(fieldName, field.Index(i))
			if err != nil {
				err = errors.WithMessagef(err, "original value:%v", field.Interface())
				return err
			}
		}
	default:
		return schema.validate(fieldName, field)
	}
	return nil

}

func (schema Schema) validate(fieldName string, field reflect.Value) error {
	// 验证 required
	isNotValid := !field.IsValid() // 空值 由nil 过来的值
	if schema.Required && (isNotValid || isEmptyValue(field, schema.ZeroAsEmpty)) {
		return fmt.Errorf("%s is required", fieldName)
	}
	if isNotValid {
		return nil // 空值不判断，因为nil代表忽略这个字段
	}

	var valStr string
	var varInt int64

	kind := field.Kind()
	length := 0
	switch kind {
	case reflect.String:
		valStr = field.String()
		length = len([]rune(valStr)) // 字符串长度中文字当做一个字符计
	case reflect.Int:
		varInt = field.Int()
	}

	// 验证 maxLength
	if schema.MaxLength > 0 && kind == reflect.String && length > schema.MaxLength {
		return fmt.Errorf("%s exceeds maximum length of %d", fieldName, schema.MaxLength)
	}
	// 验证 minLength
	if schema.MinLength > 0 && kind == reflect.String && length < schema.MinLength {
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
		err := schema.Enums.Validate(val)
		if err != nil {
			return errors.WithMessagef(err, "field name :%s", fieldName)
		}
	}
	if schema.RegExp != "" {
		ex, err := regexp.Compile(schema.RegExp)
		if err != nil {
			err = errors.WithMessagef(err, "Schema.Validate,field name:%s,regExp:%s", fieldName, schema.RegExp)
			return err
		}
		str := cast.ToString(field.Interface())
		if str != "" {
			if !ex.MatchString(str) {
				err = errors.Errorf("%s RegExp is %s,got:%s", fieldName, schema.RegExp, str)
				return err
			}
		}

	}
	return nil
}

func isEmptyValue(v reflect.Value, zeroAsEmpty bool) bool {
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
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if zeroAsEmpty {
			return v.Int() == 0
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if zeroAsEmpty {
			return v.Uint() == 0
		}
	case reflect.Float32, reflect.Float64:
		if zeroAsEmpty {
			return v.Float() == 0
		}
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

// ValueFnDBSchemaFormat 根据DB类型要求,转换数据类型
func ValueFnDBSchemaFormatType(field Field) (valueFn ValueFn) {
	return ValueFn{
		Fn: func(in any, f *Field, fs ...*Field) (any, error) {
			value := field.FormatType(in)
			return value, nil
		},
		Layer: Value_Layer_DBFormat,
	}
}

// UniqueueArray 数组元素去重
func UniqueueArray[T int | int64 | string](in any) (any, error) {
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

var ValueFnForward = ValueFn{
	Fn: func(in any, f *Field, fs ...*Field) (any, error) {
		return in, nil
	},
	Layer: Value_Layer_DBFormat,
}

// ValueFnFormatArray 格式化数组,只有一个元素时,直接返回当前元素，常用于where in 条件
var ValueFnFormatArray = ValueFn{
	Fn: func(in any, f *Field, fs ...*Field) (any, error) {
		if IsNil(in) {
			return nil, nil
		}
		valValue := reflect.Indirect(reflect.ValueOf(in))
		valType := valValue.Type()
		switch valType.Kind() {
		case reflect.Slice, reflect.Array:
			if valValue.Len() == 1 {
				in = valValue.Index(0).Interface()
			}
		}
		return in, nil
	},
	Layer: Value_Layer_DBFormat,
}

// ValueFnDecodeComma 参数中,拼接的字符串解码成数组
var ValueFnDecodeComma = ValueFn{
	Fn:    ValueFnDecodeCommaFn,
	Layer: Value_Layer_SetFormat,
}

func ValueFnDecodeCommaFn(in any, f *Field, fs ...*Field) (any, error) {
	if IsNil(in) {
		return in, nil
	}
	s := cast.ToString(in)
	if !strings.Contains(s, ",") {
		return in, nil
	}
	arr := strings.Split(s, ",")
	uniqArr, _ := UniqueueArray[string](arr) // 去重
	strArr := uniqArr.([]string)
	if len(strArr) == 1 {
		in = strArr[0] // 去重后只有一个，则转成字符串
	} else {
		in = strArr
	}
	return in, nil
}

var ValueFnShield = ValueFn{ // 屏蔽数据
	Fn:    ValueFnShieldFn,
	Layer: Value_Layer_DBFormat,
}

func ValueFnShieldFn(in any, f *Field, fs ...*Field) (any, error) {
	return nil, nil
}

// 屏蔽新增、修改 数据
var ValueFnShieldForData = ValueFnOnlyForData(ValueFnShieldFn)

var ValueFnEmpty2Nil = ValueFn{ // 空字符串改成nil,值改成nil后,sql语句中会忽略该字段,常常用在update,where 字句中
	Fn: func(in any, f *Field, fs ...*Field) (any, error) {
		switch val := in.(type) {
		case string:
			if val == "" {
				return nil, nil
			}
		case int:
			if val == 0 {
				return nil, nil
			}
		case int64:
			if val == 0 {
				return nil, nil
			}
		case []string:
			if len(val) == 0 {
				return nil, nil
			}
		case []int:
			if len(val) == 0 {
				return nil, nil
			}
		case []int64:
			if len(val) == 0 {
				return nil, nil
			}
		}
		return in, nil
	},
	Layer: Value_Layer_SetFormat,
}

var ValueFnGte = ValueFn{
	Fn: func(in any, f *Field, fs ...*Field) (value any, err error) {
		if IsNil(in) {
			return nil, nil
		}
		return Between{in}, nil
	},
	Layer: Value_Layer_DBFormat,
}

var ValueFnLte = ValueFn{
	Fn: func(in any, f *Field, fs ...*Field) (value any, err error) {
		if IsNil(in) {
			return nil, nil
		}
		return Between{nil, in}, nil
	},
	Layer: Value_Layer_DBFormat,
}

// ValueFnTrimBlankSpace 删除字符串前后空格
var ValueFnTrimBlankSpace = ValueFn{
	Fn: func(in any, f *Field, fs ...*Field) (any, error) {
		if in == nil {
			return in, nil
		}
		if str, ok := in.(string); ok {
			in = strings.Trim(str, " ")
		}
		return in, nil
	},
	Layer: Value_Layer_SetFormat,
}

var ValueFnIlike = ValueFn{
	Fn: func(in any, f *Field, fs ...*Field) (value any, err error) {
		if IsNil(in) {
			return nil, nil
		}
		str := cast.ToString(in)
		if str == "" {
			return nil, nil
		}

		return Ilike{"%", str, "%"}, nil
	},
	Layer: Value_Layer_DBFormat,
}

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
