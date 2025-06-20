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
func (oFns *ApplyFns) Append(applyFns ...ApplyFn) {
	if *oFns == nil {
		*oFns = make(ApplyFns, 0)
	}
	*oFns = append(*oFns, applyFns...)
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
		val := fmt.Sprintf("%s %s %d", f.DBColumnName().FullName(), symbol, num)
		return goqu.L(val), nil
	}
}

var ApplyFnUseDBValue ApplyFn = func(f *Field, fs ...*Field) {
	f.ValueFns.Append(ValueFn{
		Fn:    ValueFnFnUseDBValue(f, fs...),
		Layer: Value_Layer_DBFormat,
	})
}

// ApplyFnUseDBValueWhenNotEmpty 使用数据库值当数据库值不为空时,需要实现传入值不为空时更新，使用ValueFnEmpty2Nil过滤字段
func ValueFnFnUseDBValue(f *Field, fs ...*Field) ValueFnFn {
	return func(inputValue any, f *Field, fs ...*Field) (any, error) {
		if IsNil(inputValue) {
			return nil, nil
		}
		dbName := f.DBColumnName().FullNameWithQuotes()
		val := fmt.Sprintf("if(%s,%s,?)", dbName, dbName)
		return goqu.L(val, inputValue), nil
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
			ex := goqu.I(f.DBColumnName().FullName()).Gte(inputValue)
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
			ex := goqu.I(f.DBColumnName().FullName()).Lte(inputValue)
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
			column := goqu.I(f.DBColumnName().FullName())
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
		column := goqu.I(f.DBColumnName().FullName())
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

func ValueFnMustNotExists(handler Handler) ValueFn {
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
			exitstsParam := NewExistsBuilder(f.GetTable()).WithHandler(handler).AppendFields(cp)
			exists, err := exitstsParam.Exists()
			if err != nil {
				return nil, err
			}
			if exists {
				err = errors.WithMessagef(ERROR_COLUMN_VALUE_EXISTS, "column %s value %s exists", f.DBColumnName().FullName(), inputValue) // 有时存在，需要返回指定错误，方便业务自主处理错误
				return nil, err
			}
			return inputValue, err
		},
	}
	return valueFn
}

func ValueFnMustExists(handler Handler) ValueFn {
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
			exitstsParam := NewExistsBuilder(f.GetTable()).WithHandler(handler).AppendFields(cp)
			exists, err := exitstsParam.Exists()
			if err != nil {
				return nil, err
			}
			if !exists {
				err = errors.WithMessagef(ERROR_COLUMN_VALUE_NOT_EXISTS, "column %s value %s", f.DBColumnName().FullName(), inputValue) // 没有时存在，需要返回指定错误，方便业务自主处理错误
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

// Deprecated ApplyFnUnique use tableConfig.Indexs 设置unique index 代替，无需手动添加中间件

func ApplyFnUnique(handler Handler) ApplyFn { // 复合索引，给一列应用该中间件即可
	return func(f *Field, fs ...*Field) {
		f.SceneInsert(func(f *Field, fs ...*Field) {
			f.ValueFns.Append(ValueFnApiValidate(func(inputValue any, f *Field, fs ...*Field) (any, error) {
				err := f.GetTable().CheckUniqueIndex(fs...)
				return inputValue, err
			}))
		})
		f.SceneUpdate(func(f *Field, fs ...*Field) {
			f.ShieldUpdate(true)
			f.WhereFns.Append(ValueFnForward)
		})
		f.SceneSelect(func(f *Field, fs ...*Field) {
			f.WhereFns.Append(ValueFnEmpty2Nil)
		})
	}
}

// Deprecated ApplyFnUniqueField 废弃，直接使用 ApplyFnUnique 即可
//
//	func ApplyFnUniqueField(handler Handler) ApplyFn {
//		return ApplyFnUnique(handler)
//	}
//
// ApplyFnUpdateIfNull 数据表记录字段为null时，更新为输入值,否则不更新，后续改成 set `feild_x`=if(`feild_x`,`feild_x`,?)
// Deprecated 废弃，直接使用 ValueFnUpdateIfFalse 代替
func ApplyFnUpdateIfNull(table TableConfig, handler Handler) ApplyFn {
	return func(f *Field, fs ...*Field) {
		f.SceneUpdate(func(f *Field, fs ...*Field) {
			f.ValueFns.Append(ValueFn{
				Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
					if IsNil(inputValue) {
						return inputValue, nil
					}

					cpFields := Fields(fs).Copy()
					if len(cpFields) > 0 {
						cpFields[0].SetSelectColumns(f.DBColumnName().FullName())
					}
					var dbValue any
					exists, err := NewFirstBuilder(table).WithHandler(handler).AppendFields(cpFields...).First(&dbValue)
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
