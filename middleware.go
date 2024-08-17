package sqlbuilder

import (
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"github.com/spf13/cast"
)

type MiddlewareFn func(f *Field, fs ...*Field)

func (oFn MiddlewareFn) Apply(f *Field, fs ...*Field) {
	oFn(f, fs...)
}

type MiddlewareFns []MiddlewareFn

func (oFns MiddlewareFns) Apply(f *Field, fs ...*Field) {
	for _, oFn := range oFns {
		oFn(f, fs...)
	}
}

type SceneFn struct {
	Name  string
	Scene Scene
	Fn    MiddlewareFn
}

type SceneFns []SceneFn

func (sceneFns SceneFns) GetByScene(scene Scene) SceneFns {
	sub := make(SceneFns, 0)
	for _, s := range sceneFns {
		if scene.Is(s.Scene) {
			sub.Append(s)
		}
	}
	return sub
}
func (sceneFns *SceneFns) Remove(name string) {
	tmp := make(SceneFns, 0)
	for _, scenaFn := range *sceneFns {
		if scenaFn.Name == name {
			continue
		}
		tmp.Append(scenaFn)
	}
	*sceneFns = tmp
}

// Append 常规添加
func (sis *SceneFns) Append(sceneFns ...SceneFn) {
	if *sis == nil {
		*sis = make(SceneFns, 0)
	}
	*sis = append(*sis, sceneFns...)

}

// IncreaseMiddleware 字段值递增中间件
var IncreaseMiddleware MiddlewareFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(func(inputValue any) (any, error) {
		val := fmt.Sprintf("`%s`+1", f.DBName())
		return goqu.L(val), nil
	})
}

var MiddlewareFnWhereIlike MiddlewareFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(ValueFnWhereLike)
}

var MiddlewareFnOrderDesc MiddlewareFn = func(f *Field, fs ...*Field) {
	f._OrderFn = OrderFnDesc
}
var MiddlewareFnOrderAsc MiddlewareFn = func(f *Field, fs ...*Field) {
	f._OrderFn = OrderFnAsc
}

func MiddlewareFnOrderField(valueOrder ...any) MiddlewareFn {
	return func(f *Field, fs ...*Field) {
		f._OrderFn = OrderFieldFn(valueOrder...)
	}
}

var MiddlewareFnWhereGte MiddlewareFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(func(inputValue any) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		ex := goqu.C(f.DBName()).Gte(inputValue)
		return ex, nil
	})
}

var MiddlewareFnWhereLte MiddlewareFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(func(inputValue any) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		ex := goqu.C(f.DBName()).Lte(inputValue)
		return ex, nil
	})
}

// MiddlewareFnWhereFindInColumnSet 传入的值在列字段集合内
var MiddlewareFnWhereFindInColumnSet MiddlewareFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(func(inputValue any) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		val := cast.ToString(inputValue)
		column := goqu.C(f.DBName())
		expression := goqu.L("FIND_IN_SET(?,?)", val, column)
		return expression, nil
	})
}

// MiddlewareFnWhereFindInValueSet 列字段值在传入的集合内
var MiddlewareFnWhereFindInValueSet MiddlewareFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(func(inputValue any) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		val := cast.ToString(inputValue)
		column := goqu.C(f.DBName())
		expression := goqu.L("FIND_IN_SET(?,?)", column, val)
		return expression, nil
	})
}

var MiddlewareFnValueFormatBySchemaType MiddlewareFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(func(inputValue any) (any, error) {
		value := f.FormatType(inputValue)
		return value, nil
	})
}

var MiddlewareFnValueFnTrimSpace MiddlewareFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(ValueFnTrimBlankSpace, ValueFnEmpty2Nil)
}
