package sqlbuilder

import (
	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/funcs"
)

type TableConfig struct {
	Name                     string
	alias                    string
	Columns                  ColumnConfigs // 后续吧table 纳入，通过 Column.Identity 生成 Field 操作
	FieldName2DBColumnNameFn FieldName2DBColumnNameFn
}

type TableConfigs []TableConfig

func (ts TableConfigs) GetByName(name string) (t *TableConfig, exists bool) {
	if name == "" {
		return nil, false
	}
	t, exists = funcs.GetOne(ts, func(t TableConfig) bool { return t.Name == name })
	return t, exists
}

func (t TableConfig) Copy() TableConfig {
	cp := t
	return cp // 方便后续增加[]slice 时复制扩展
}

func (t TableConfig) AliasString() string {
	alias := t.alias
	if alias == "" {
		alias = t.Name
	}
	return alias
}
func (t TableConfig) Alias() (aliasExpression exp.AliasedExpression) {
	return exp.NewAliasExpression(t.Table(), t.AliasString()) // 默认返回表名作为别名
}

func (t TableConfig) WithAlias(alias string) TableConfig {
	t.alias = alias
	return t
}

func (t TableConfig) Table() exp.IdentifierExpression {
	table := goqu.T(t.Name)
	return table
}

func (t TableConfig) IsNil() bool {
	return t.Name == ""
}

// Merge 合并表配置信息,同名覆盖，别名同名覆盖,a.Merge(b) 实现b覆盖a; b.Merge(a)、a.Merge(b,a) 可实现a 覆盖b
func (t TableConfig) Merge(tables ...TableConfig) TableConfig {
	for _, table := range tables {
		if table.Name != "" {
			t.Name = table.Name
		}
		if table.alias != "" {
			t.alias = table.alias
		}
	}
	return t
}

type ColumnConfig struct {
	Name     string     `json:"name"` // 驼峰,程序中使用
	Type     SchemaType `json:"type"`
	Length   int        `json:"length"`
	PK       bool       `json:"pk"`
	Unique   bool       `json:"unique"`
	Nullable bool       `json:"nullable"`
	Default  any        `json:"default"`
	Comment  string     `json:"comment"`
	Enums    Enums      `json:"enums"`
}

func (c ColumnConfig) CamelName() string {
	return funcs.CamelCase(c.Name, false, false)
}

func (c ColumnConfig) MakeField(value any) *Field {
	valueFnFn := func(_ any, f *Field, fs ...*Field) (any, error) {
		return value, nil
	}
	f := NewField(valueFnFn).SetName(c.CamelName()).SetType(c.Type).Comment(c.Comment).AppendEnum(c.Enums...).SetDefault(c.Default)
	if c.Type.IsEqual(Schema_Type_string) {
		f.SetLength(c.Length)
	}
	//todo 更多细节设置,如根据默认值和Nullable设置是否容许为空等
	return f
}

type ColumnConfigs []ColumnConfig

// GetByIdentity  通过标识获取列配置信息，找不到则panic退出。主要用于生成字段时快速定位列配置信息。
func (cs ColumnConfigs) GetByName(name string) (c ColumnConfig) {
	for _, c := range cs {
		if c.Name == name {
			return c
		}
	}
	err := errors.Errorf("ColumnConfig not found by identity: " + string(name))
	panic(err)
}
