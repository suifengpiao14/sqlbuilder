package sqlbuilder

import "reflect"

var Time_format = "2024-01-02 15:04:05"

// Column 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Column struct {
	Name  string `json:"name"`
	Value func(in any) any
}

func NewColumn(name string, value func(in any) any) Column {
	if value == nil {
		value = func(in any) any {
			return in
		}
	}
	column := Column{
		Name:  name,
		Value: value,
	}
	return column
}

type ColumnFn func() []Column

func (fn ColumnFn) Data() (data any, err error) {
	m := map[string]any{}
	columns := fn()
	for _, c := range columns {
		if c.Name != "" {
			m[c.Name] = c.Value(nil)
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
