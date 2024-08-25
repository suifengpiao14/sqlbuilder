package sqlbuilder

import (
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

type ApplyFn func(f *Field, fs ...*Field)

func (oFn ApplyFn) Apply(f *Field, fs ...*Field) {
	oFn(f, fs...)
}

type ApplyFns []ApplyFn

func (oFns ApplyFns) Apply(f *Field, fs ...*Field) {
	for _, oFn := range oFns {
		oFn(f, fs...)
	}
}

type SceneFn struct {
	Name  string
	Scene Scene
	Fn    ApplyFn
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

// ApplyFnIncrease 字段值递增中间件
var ApplyFnIncrease ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(func(inputValue any) (any, error) {
		val := fmt.Sprintf("`%s`+1", f.DBName())
		return goqu.L(val), nil
	})
}

var ApplyFnWhereIlike ApplyFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(ValueFnWhereLike)
}

var ApplyFnOrderDesc ApplyFn = func(f *Field, fs ...*Field) {
	f._OrderFn = OrderFnDesc
}
var ApplyFnOrderAsc ApplyFn = func(f *Field, fs ...*Field) {
	f._OrderFn = OrderFnAsc
}

func ApplyFnOrderField(valueOrder ...any) ApplyFn {
	return func(f *Field, fs ...*Field) {
		f._OrderFn = OrderFieldFn(valueOrder...)
	}
}

var ApplyFnWhereGte ApplyFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(func(inputValue any) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		ex := goqu.C(f.DBName()).Gte(inputValue)
		return ex, nil
	})
}

var ApplyFnWhereLte ApplyFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(func(inputValue any) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		ex := goqu.C(f.DBName()).Lte(inputValue)
		return ex, nil
	})
}

// ApplyFnWhereFindInColumnSet 传入的值在列字段集合内
var ApplyFnWhereFindInColumnSet ApplyFn = func(f *Field, fs ...*Field) {
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

// ApplyFnWhereFindInValueSet 列字段值在传入的集合内
var ApplyFnWhereFindInValueSet ApplyFn = func(f *Field, fs ...*Field) {
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

var ApplyFnValueFormatBySchemaType ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(func(inputValue any) (any, error) {
		value := f.FormatType(inputValue)
		return value, nil
	})
}

var ApplyFnValueFnTrimSpace ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(ValueFnTrimBlankSpace)
}

var ApplyFnValueEmpty2Nil ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(ValueFnEmpty2Nil)
}

// ApplyFnUniqueField 单列唯一索引键,新增场景中间件
func ApplyFnUniqueField(table string, queryFn QueryHandler) ApplyFn {
	return func(f *Field, fs ...*Field) {
		sceneFnName := "checkexists"
		sceneFn := SceneFn{
			Name:  sceneFnName,
			Scene: SCENE_SQL_INSERT,
			Fn: func(f *Field, fs ...*Field) {
				f1 := f.Copy()               //复制不影响外部,在内部copy 是运行时 copy,确保 builder阶段的设置都能考呗到
				f1.SceneFnRmove(sceneFnName) // 避免死循环
				f1.WhereFns.Append(ValueFnForward)
				f.ValueFns.Append(func(inputValue any) (any, error) {
					exitstsParam := NewExistsBuilder(table).AppendFields(f1)
					exists, err := exitstsParam.Exists(queryFn)
					if err != nil {
						return nil, err
					}
					if exists {
						err = errors.Errorf("unique column %s value %s exists", f1.DBName(), inputValue)
						return nil, err
					}
					return inputValue, nil
				})

			},
		}
		f.SceneFn(sceneFn)
		f.SceneUpdate(func(f *Field, fs ...*Field) {
			f.WhereFns.Append(ValueFnForward)
		})
		f.SceneSelect(func(f *Field, fs ...*Field) {
			f.WhereFns.Append(ValueFnEmpty2Nil)
		})
	}
}

func ApplyFnUpdateIfNull(table string, firstHandler FirstHandler) ApplyFn {
	return func(f *Field, fs ...*Field) {
		f.SceneUpdate(func(f *Field, fs ...*Field) {
			f.ValueFns.Append(func(inputValue any) (any, error) {
				if IsNil(inputValue) {
					return inputValue, nil
				}

				cpFields := Fields(fs).Copy()
				if len(cpFields) > 0 {
					cpFields[0].SetSelectColumns(f.DBName())
				}
				var dbValue any
				exists, err := NewFirstBuilder(table).AppendFields(cpFields...).First(&dbValue, firstHandler)
				if err != nil {
					return nil, err
				}
				if !exists {
					return inputValue, err
				}
				shieldUpdate := false
				if !IsNil(dbValue) {
					switch val := dbValue.(type) {
					case int:
						shieldUpdate = val > 0
					case int64:
						shieldUpdate = val > 0
					case string:
						shieldUpdate = val != ""
					}
				}
				f.ShieldUpdate(shieldUpdate) // 设置是否屏蔽更新
				return inputValue, nil
			})
		})
	}
}
