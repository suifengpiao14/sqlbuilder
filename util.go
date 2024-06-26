package sqlbuilder

import "reflect"

var Time_format = "2024-01-02 15:04:05"

// Field 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Field struct {
	Title   string                              `json:"title"`
	Name    string                              `json:"name"`
	Value   func(in any) (value any, err error) // 增加error，方便封装字段验证规则
	Migrate func(table string, options ...MigrateOptionI) Migrates
}

func NewField(name string, value func(in any) (any, error)) Field {
	if value == nil {
		value = func(in any) (any, error) {
			return in, nil
		}
	}
	column := Field{
		Name:  name,
		Value: value,
	}
	return column
}

type FieldFn func() []Field

func (fn FieldFn) Data() (data any, err error) {
	m := map[string]any{}
	columns := fn()
	for _, c := range columns {
		if c.Name != "" {
			val, err := c.Value(nil)
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
