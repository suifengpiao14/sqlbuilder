package sqlbuilder

import (
	"fmt"
	"slices"
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/funcs"
)

type DBName struct {
	Name  string `json:"name"`
	Alias string `json:"alias"`
}

func (n DBName) BaseName() string {
	if n.Alias != "" {
		return n.Alias
	}
	return n.Name
}

func (n DBName) BaseNameWithQuotes() string {
	name := n.BaseName()
	if name == "" {
		return ""
	}
	nameWithQuotes := fmt.Sprintf("`%s`", name)
	return nameWithQuotes
}

func (n DBName) IsNil() bool {
	return n.BaseName() == ""
}

type DBIdentifier []DBName

func (id DBIdentifier) BaseName() string {
	cp := make(DBIdentifier, len(id))
	copy(cp, id)
	slices.Reverse(cp)
	cp = funcs.Filter(cp, func(n DBName) bool {
		return !n.IsNil()
	})
	if len(cp) == 0 {
		return ""
	}
	baseName := cp[0].BaseName()
	return baseName
}

func (id DBIdentifier) FullName() string {
	names := funcs.Filter(id, func(s DBName) bool {
		return !s.IsNil()
	})
	arr := funcs.Map(names, func(s DBName) string {
		return s.BaseName()
	})
	return strings.Join(arr, ".")
}

func (i DBIdentifier) NameWithQuotes() string {
	name := i.BaseName()
	if name == "" {
		return ""
	}
	nameWithQuotes := fmt.Sprintf("`%s`", name)
	return nameWithQuotes
}

func (id DBIdentifier) FullNameWithQuotes() string {
	names := funcs.Filter(id, func(s DBName) bool {
		return !s.IsNil()
	})
	arr := funcs.Map(names, func(s DBName) string {
		return s.BaseNameWithQuotes()
	})
	return strings.Join(arr, ".")
}

type SchemaConfig struct {
	DBName
}

type TableConfig struct {
	DBName
	Columns                  ColumnConfigs            // 后续吧table 纳入，通过 Column.Identity 生成 Field 操作
	FieldName2DBColumnNameFn FieldName2DBColumnNameFn `json:"-"`
	Schema                   SchemaConfig
}

func NewTableConfig(name string) TableConfig {
	return TableConfig{
		DBName: DBName{Name: name},
	}
}

func (t TableConfig) WithFieldName2DBColumnNameFn(convertFn FieldName2DBColumnNameFn) TableConfig {
	t.FieldName2DBColumnNameFn = convertFn
	return t
}

func (t TableConfig) GetFullName() string {
	identifier := DBIdentifier{
		t.Schema.DBName,
		t.DBName,
	}
	return identifier.FullName()
}

func (t TableConfig) FullNameWithQuotes() string {
	identifier := DBIdentifier{
		t.Schema.DBName,
		t.DBName,
	}
	return identifier.FullNameWithQuotes()
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
	copy(cp.Columns, t.Columns)
	return cp // 方便后续增加[]slice 时复制扩展
}

func (t TableConfig) AliasString() string {
	return t.Alias
}

func (t TableConfig) WithAlias(alias string) TableConfig {
	t.Alias = alias
	return t
}

func (t TableConfig) AliasOrTableExpr() exp.Expression { // 有别名，返回别名，没有返回表名
	table := goqu.T(t.Name)
	if t.Alias == "" {
		return table
	}
	alias := table.As(t.Alias)
	return alias
}
func (t TableConfig) AliasExpr() exp.AliasedExpression { // 有时候需要独立获取别名表达式，如 select alias.* from a as alias; ,生成alias.*

	alias := goqu.T(t.Name).As(t.Alias)
	return alias
}

func (t TableConfig) IsNil() bool {
	return t.Name == ""
}

// Merge 合并表配置信息,同名覆盖，别名同名覆盖,a.Merge(b) 实现b覆盖a; b.Merge(a)、a.Merge(b,a) 可实现a 覆盖b
func (t TableConfig) Merge(tables ...TableConfig) TableConfig {
	for _, table := range tables {
		if t.Name != "" && table.Name != t.Name { //表名存在并且不同，忽略合并操作，表名不存在，使用第一个表名作为基准表名
			continue
		}

		if table.Name != "" {
			t.Name = table.Name
		}
		if table.Alias != "" {
			t.Alias = table.Alias
		}
		if table.FieldName2DBColumnNameFn != nil {
			t.FieldName2DBColumnNameFn = table.FieldName2DBColumnNameFn
		}
		if table.Columns != nil {
			t.Columns = t.Columns.Merge(table.Columns...)
		}
	}
	return t
}

type BusinessIdentity string // 业务标识,是关联数据表字段和Field的桥梁，是固定不变的，是模型组合的核心标识，始终保持不变（具体细化、应用待实践）

type ColumnConfig struct {
	BusinessIdentity BusinessIdentity // 业务标识
	Name             string           `json:"name"` // 驼峰,程序中使用
	Type             SchemaType       `json:"type"`
	Length           int              `json:"length"`
	PK               bool             `json:"pk"`
	Unique           bool             `json:"unique"`
	Nullable         bool             `json:"nullable"`
	Default          any              `json:"default"`
	Comment          string           `json:"comment"`
	Enums            Enums            `json:"enums"`
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

func (cs ColumnConfigs) Merge(others ...ColumnConfig) ColumnConfigs {
	cs = append(cs, others...)
	return cs
}

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
