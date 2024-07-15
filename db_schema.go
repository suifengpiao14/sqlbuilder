package sqlbuilder

import (
	"fmt"
	"reflect"
	"strings"

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

	Primary      bool `json:"primary"`      //是否为主键
	Unique       bool `json:"unique"`       // 是否为唯一键
	ShieldUpdate bool `json:"shieldUpdate"` //屏蔽更新该字段,适合不可更新字段,如tenat,deleted_at
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
	Schema_Type_int    SchemaType = "int"
)

type Enums []Enum

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

func (es Enums) ValuesStr() (valuesStr []string) {
	values := es.Values()
	valuesStr = make([]string, 0)
	for _, v := range values {
		valuesStr = append(valuesStr, cast.ToString(v))
	}
	return valuesStr
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
		values = append(values, fmt.Sprintf("%s-%s", e.Key, e.Title))
	}
	return fmt.Sprintf("(%s)", strings.Join(values, ","))
}

type Enum struct {
	Title     string `json:"title"`
	Key       any    `json:"key"`
	IsDefault bool   `json:"isDefault"`
}

func (e Enum) IsEqual(val any) (ok bool) {
	ok = strings.EqualFold(cast.ToString(e.Key), cast.ToString(val))
	return ok
}

func (schema Schema) Validate(fieldName string, field reflect.Value) error {
	// 验证 required
	if schema.Required && isEmptyValue(field) {
		return fmt.Errorf("%s is required", fieldName)
	}
	// 验证 maxLength
	if schema.MaxLength > 0 && field.Kind() == reflect.String && len(field.String()) > schema.MaxLength {
		return fmt.Errorf("%s exceeds maximum length of %d", fieldName, schema.MaxLength)
	}
	// 验证 minLength
	if schema.MinLength > 0 && field.Kind() == reflect.String && len(field.String()) < schema.MinLength {
		return fmt.Errorf("%s is less than minimum length of %d", fieldName, schema.MinLength)
	}
	// 验证 maximum
	if schema.Maximum > 0 && field.Kind() == reflect.Int && field.Int() > int64(schema.Maximum) {
		return fmt.Errorf("%s exceeds maximum value of %d", fieldName, schema.Maximum)
	}
	// 验证 minimum
	if schema.Minimum > 0 && field.Kind() == reflect.Int && field.Int() < int64(schema.Minimum) {
		return fmt.Errorf("%s is less than minimum value of %d", fieldName, schema.Minimum)
	}
	// 验证 enums
	if len(schema.Enums) > 0 {
		if !contains(schema.Enums.ValuesStr(), cast.ToString(field.Interface())) {
			return fmt.Errorf("%s must be one of %v", fieldName, schema.Enums.Values())
		}
	}
	return nil
}

func isEmptyValue(v reflect.Value) bool {
	return cast.ToString(v.Interface()) == ""
	/*
		switch v.Kind() {
		   	case reflect.String:
		   		return v.Len() == 0
		   	case reflect.Array, reflect.Slice, reflect.Map, reflect.Chan:
		   		return v.Len() == 0
		   	case reflect.Bool:
		   		return !v.Bool()
		   	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		   		return v.Int() == 0
		   	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		   		return v.Uint() == 0
		   	case reflect.Float32, reflect.Float64:
		   		return v.Float() == 0
		   	case reflect.Interface, reflect.Ptr:
		   		return v.IsNil()
		   	}
		return false
	*/
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// ValueFnDBSchemaValidator 将DBSchema validator 封装成 ValueFn 中间件函数
func ValueFnDBSchemaValidator(field Field) (valueFn ValueFn) {
	return func(in any) (any, error) {
		err := field.Validate(in)
		if err != nil {
			return nil, err
		}
		return in, nil
	}
}

// ValueFnDBSchemaFormat 根据DB类型要求,转换数据类型
func ValueFnDBSchemaFormatType(field Field) (valueFn ValueFn) {
	return func(in any) (any, error) {
		value := field.FormatType(in)
		return value, nil
	}
}

func ValueFnForward(in any) (any, error) {
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
