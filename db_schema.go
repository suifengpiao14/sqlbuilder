package sqlbuilder

import (
	"fmt"
	"reflect"

	"github.com/spf13/cast"
)

// 基于数据表填充对应数据，同时也可以基于此生成SQL DDL
type DBSchema struct {
	Title    string `json:"title"`
	Required bool   `json:"required,string"` // 对应数据库的not null
	//AllowEmpty bool   `json:"allowEmpty,string"` // 是否可以为空 空定义：转为为字符串后为"",数字0不是空字符 通过最小长度 1 表达
	Comment   string `json:"comment"`
	Type      string `json:"type"`
	Default   any    `json:"default"`
	Enums     Enums  `json:"enums"`
	MaxLength int    `json:"maxLength"` // 字符串最大长度
	MinLength int    `json:"minLength"` // 字符串最小长度
	Maximum   int    `json:"maximum"`   // 数字最大值
	Minimum   int    `json:"minimum"`   // 数字最小值
	RegExp    string `json:"regExp"`    //正则表达式
}

const (
	DBSchema_Type_string = "string"
	DBSchema_Type_int    = "int"
	DBSchema_Type_phone  = "phone"
	DBSchema_Type_email  = "email"
	DBSchema_Type_Enum   = "enum"
)

type Enums []Enum

func (es Enums) Values() (values []string) {
	values = make([]string, 0)
	for _, e := range es {
		values = append(values, e.Key)
	}
	return values

}

type Enum struct {
	Title string `json:"title"`
	Key   string `json:"key"`
}

func (schema DBSchema) Validate(fieldName string, field reflect.Value) error {
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
		if !contains(schema.Enums.Values(), cast.ToString(field.Interface())) {
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
