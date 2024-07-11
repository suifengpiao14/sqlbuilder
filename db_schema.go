package sqlbuilder

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/cast"
)

// 基于数据表填充对应数据，同时也可以基于此生成SQL DDL
type Schema struct {
	Title     string `json:"title"`
	Required  bool   `json:"required,string"` // 对应数据库的not null
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

// AllowEmpty 是否可以为空
func (dbSchema Schema) AllowEmpty() bool {
	return dbSchema.MinLength < 1 && dbSchema.Type == DBSchema_Type_string
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

// String 生成文档有使用
func (es Enums) String() (str string) {
	values := make([]string, 0)
	for _, e := range es {
		values = append(values, fmt.Sprintf("%s-%s", e.Key, e.Title))
	}
	return strings.Join(values, " ")
}

type Enum struct {
	Title string `json:"title"`
	Key   string `json:"key"`
}

type ValidatorI interface {
	Name() string
	ValidateI
}

type ValidatorIs []ValidatorI

func (vis ValidatorIs) GetByNames(names ...string) (validatorIs ValidatorIs) {
	validatorIs = make(ValidatorIs, 0)
	for _, vi := range vis {
		for _, name := range names {
			if strings.EqualFold(vi.Name(), name) {
				validatorIs = append(validatorIs, vi)
			}
		}

	}
	return validatorIs
}
func (vis ValidatorIs) Validate(val any) (err error) {
	for _, vi := range vis {
		err = vi.Validate(val)
		if err != nil {
			return err
		}
	}
	return nil
}

var validatorIs = make(ValidatorIs, 0)

func RegisterValidator(validators ...ValidatorI) {
	validatorIs = append(validatorIs, validators...)
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

// ValueFnEmptyStr2Nil 空字符串改成nil,值改成nil后,sql语句中会忽略该字段,常常用在update,where 字句中
func ValueFnEmptyStr2Nil(field Field, exceptFileds ...Field) (valueFn ValueFn) {
	return func(in any) (any, error) {
		if Fields(exceptFileds).Contains(field) {
			return in, nil
		}
		str := cast.ToString(in)
		if str == "" {
			return nil, nil
		}
		return in, nil
	}
}
