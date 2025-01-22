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
	f.ValueFns.Append(ValueFn{
		Fn:    ValueFnFnIncrease(f, fs...),
		Layer: Value_Layer_DBFormat,
	})
}

func ValueFnFnIncrease(f *Field, fs ...*Field) ValueFnFn {
	return func(inputValue any, f *Field, fs ...*Field) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		num := cast.ToInt(inputValue)
		if num == 0 {
			return nil, nil
		}
		symbol := "+"
		if num < 0 {
			symbol = "-"
		}
		val := fmt.Sprintf("%s %s %d", f.DBName(), symbol, num)
		return goqu.L(val), nil
	}
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
	f.WhereFns.Append(ValueFn{
		Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
			if IsNil(inputValue) {
				return nil, nil
			}
			ex := goqu.I(f.DBName()).Gte(inputValue)
			return ex, nil
		},
		Layer: Value_Layer_DBFormat,
	})
}

var ApplyFnWhereLte ApplyFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(ValueFn{
		Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
			if IsNil(inputValue) {
				return nil, nil
			}
			ex := goqu.I(f.DBName()).Lte(inputValue)
			return ex, nil
		},
		Layer: Value_Layer_DBFormat,
	})
}

// ApplyFnWhereFindInColumnSet 传入的值在列字段集合内
var ApplyFnWhereFindInColumnSet ApplyFn = func(f *Field, fs ...*Field) {
	f.WhereFns.Append(ValueFn{
		Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
			if IsNil(inputValue) {
				return nil, nil
			}
			val := cast.ToString(inputValue)
			column := goqu.I(f.DBName())
			expression := goqu.L("FIND_IN_SET(?,?)", val, column)
			return expression, nil
		},
		Layer: Value_Layer_DBFormat,
	})
}

// ApplyFnValueFnSetIfEmpty 数据库值为空则修改,否则不修改,用于update
var ApplyFnValueFnSetIfEmpty ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.Append(ValueFnOnlyForData(func(inputValue any, f *Field, fs ...*Field) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		column := goqu.I(f.DBName())
		expression := goqu.L("if(?,?,?) ", column, column, inputValue)
		return expression, nil
	}))
}

var ApplyFnValueFormatBySchemaType ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.Append(ValueFn{
		Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
			value := f.FormatType(inputValue)
			return value, nil
		},
		Layer: Value_Layer_DBFormat,
	})
}

var ApplyFnValueFnTrimSpace ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.Append(ValueFnTrimBlankSpace)
}

var ApplyFnValueEmpty2Nil ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.Append(ValueFnEmpty2Nil)
}

func ValueFnMustNotExists(existsFn ExistsHandler) ValueFn {
	var valueFn ValueFn
	valueFn = ValueFn{
		Name:  "mustnotexists",
		Layer: Value_Layer_ApiValidate,
		Order: 2,
		Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
			if IsNil(inputValue) {
				return nil, nil
			}
			cp := f.Copy()
			cp.ValueFns.Remove(valueFn)
			exitstsParam := NewExistsBuilder(f.GetTable()).WithHandler(existsFn).AppendFields(cp)
			exists, err := exitstsParam.Exists()
			if err != nil {
				return nil, err
			}
			if exists {
				err = errors.WithMessagef(ERROR_COLUMN_VALUE_EXISTS, "column %s value %s exists", f.DBName(), inputValue) // 有时存在，需要返回指定错误，方便业务自主处理错误
				return nil, err
			}
			return inputValue, err
		},
	}
	return valueFn
}

func ValueFnMustExists(existsFn ExistsHandler) ValueFn {
	var valueFn ValueFn
	valueFn = ValueFn{
		Name:  "mustexists",
		Layer: Value_Layer_ApiValidate,
		Order: 2,
		Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
			if IsNil(inputValue) {
				return nil, nil
			}
			cp := f.Copy()
			cp.ValueFns.Remove(valueFn)
			exitstsParam := NewExistsBuilder(f.GetTable()).WithHandler(existsFn).AppendFields(cp)
			exists, err := exitstsParam.Exists()
			if err != nil {
				return nil, err
			}
			if !exists {
				err = errors.WithMessagef(ERROR_COLUMN_VALUE_NOT_EXISTS, "column %s value %s exists", f.DBName(), inputValue) // 没有时存在，需要返回指定错误，方便业务自主处理错误
				return nil, err
			}
			return inputValue, err
		},
	}
	return valueFn
}

var ERROR_COLUMN_VALUE_NOT_EXISTS = errors.New("column value not exists")
var ERROR_COLUMN_VALUE_EXISTS = errors.New("column value exists")
var ERROR_Unique = errors.New("unique error")

func ApplyFnUnique(existsFn ExistsHandler) ApplyFn { // 复合索引，给一列应用该中间件即可
	return func(f *Field, fs ...*Field) {
		sceneFnName := "checkexists"
		sceneFn := SceneFn{
			Name:  sceneFnName,
			Scene: SCENE_SQL_INSERT,
			Fn: func(f *Field, fs ...*Field) {
				allFields := Fields(fs)
				f1 := f.Copy() //复制不影响外部,在内部copy 是运行时 copy,确保 builder阶段的设置都能考呗到
				table := f1.GetTable()
				f1.SetRequired(true) // 新增场景 设置必填
				uniqueFields := allFields.GetByIndex(f1.GetIndexs().GetUnique()...).Copy()
				uniqueFields.Replace(f1) //替换成当前 f1 字段
				uniqueFields.Apply(func(f *Field, fs ...*Field) {
					f.ShieldUpdate(true)
					f.SceneFnRmove(sceneFnName) // 避免死循环
					f.WhereFns.Append(ValueFnForward)
				})
				f.ValueFns.Append(ValueFn{
					Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
						exitstsParam := NewExistsBuilder(table).WithHandler(existsFn).AppendFields(uniqueFields...)
						exists, err := exitstsParam.Exists()
						if err != nil {
							return nil, err
						}
						if exists {
							err = errors.WithMessagef(ERROR_Unique, "unique column %s value %s exists", f1.DBName(), inputValue) // 有时存在，需要返回指定错误，方便业务自主处理错误（如批量新增，存在忽略即可）
							return nil, err
						}
						return inputValue, nil
					},
					Layer: Value_Layer_ApiValidate,
					Order: 1, //schemna 验证完后再执行，避免提前校验导致错误信息不准确

				})

			},
		}
		f.SceneFn(sceneFn)
		f.SceneUpdate(func(f *Field, fs ...*Field) {
			f.ShieldUpdate(true)
			f.WhereFns.Append(ValueFnForward)
		})
		f.SceneSelect(func(f *Field, fs ...*Field) {
			f.WhereFns.Append(ValueFnEmpty2Nil)
		})
	}
}

// Deprecated ApplyFnUniqueField 单列唯一索引键,新增场景中间件
func ApplyFnUniqueField(table string, existsFn ExistsHandler) ApplyFn {
	return ApplyFnUnique(existsFn)
}

func ApplyFnUpdateIfNull(table string, firstHandler FirstHandler) ApplyFn {
	return func(f *Field, fs ...*Field) {
		f.SceneUpdate(func(f *Field, fs ...*Field) {
			f.ValueFns.Append(ValueFn{
				Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
					if IsNil(inputValue) {
						return inputValue, nil
					}

					cpFields := Fields(fs).Copy()
					if len(cpFields) > 0 {
						cpFields[0].SetSelectColumns(f.DBName())
					}
					var dbValue any
					exists, err := NewFirstBuilder(table).WithHandler(firstHandler).AppendFields(cpFields...).First(&dbValue)
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
				},
				Layer: Value_Layer_DBFormat,
			})
		})
	}
}
