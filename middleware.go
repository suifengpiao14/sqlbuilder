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

type SceneMiddlewareFn struct {
	Scene        Scene
	MiddlewarFns MiddlewareFns
}

type SceneMiddlewareFns []SceneMiddlewareFn

func (sceneMiddlewareFns SceneMiddlewareFns) GetByScene(scene Scene) SceneMiddlewareFn {
	for _, s := range sceneMiddlewareFns {
		if scene.Is(s.Scene) {
			return s
		}
	}
	return SceneMiddlewareFn{
		Scene: scene,
	}
}

// Append 常规添加
func (sis *SceneMiddlewareFns) Append(sceneMiddlewareFns ...SceneMiddlewareFn) {
	if *sis == nil {
		*sis = make(SceneMiddlewareFns, 0)
	}
	for _, sceneInit := range sceneMiddlewareFns {
		exists := false
		for i := 0; i < len(*sis); i++ {
			if (*sis)[i].Scene.Is(sceneInit.Scene) {
				if (*sis)[i].MiddlewarFns == nil {
					(*sis)[i].MiddlewarFns = make(MiddlewareFns, 0)
				}
				(*sis)[i].MiddlewarFns = append((*sis)[i].MiddlewarFns, sceneInit.MiddlewarFns...)
				exists = true
				break
			}
		}
		if !exists {
			*sis = append(*sis, sceneInit)
		}

	}

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
