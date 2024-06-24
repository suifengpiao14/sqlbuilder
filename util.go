package sqlbuilder

import "reflect"

var Time_format = "2024-01-02 15:04:05"

//WrapFieldName 业务组件的字段名称全部改为内置，并约定调用该函数生成，使用者可以重写这个函数实现字段名定制化
var WrapFieldName = func(fieldName string, business string) (wrappedFieldName string) {
	return wrappedFieldName
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
