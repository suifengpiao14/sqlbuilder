package sqlbuilder

import (
	"context"
	"encoding/json"
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

type CustomTableConfigFn[T any] func(original T) (customd T)

func (fn CustomTableConfigFn[T]) Apply(original T) (customd T) {
	if IsNil(fn) {
		return original
	}
	customd = fn(original)
	return customd
}

type TableConfig struct {
	DBName
	Columns                  ColumnConfigs            // 后续吧table 纳入，通过 Column.Identity 生成 Field 操作
	FieldName2DBColumnNameFn FieldName2DBColumnNameFn `json:"-"`
	Schema                   SchemaConfig
	handler                  Handler
	Indexs                   Indexs // 索引信息，唯一索引，在新增时会自动校验是否存在,更新时会自动保护
	// 表级别的字段（值产生方式和实际数据无关），比如创建时间、更新时间、删除字段等，这些字段设置好后，相关操作可从此获取字段信息,增加该字段，方便封装delete操作、冗余字段自动填充等操作, 增加ctx 入参 方便使用ctx专递数据，比如 业务扩展多租户，只需表增加相关字段，在ctx中传递租户信息，并设置表级别字段场景即可
	TableLevelFieldsHook func(ctx context.Context, fs ...*Field) (hookedFields Fields)
}

func NewTableConfig(name string) TableConfig {
	return TableConfig{
		DBName: DBName{Name: name},
		TableLevelFieldsHook: func(ctx context.Context, fs ...*Field) (hookedFields Fields) {
			return
		},
	}
}

func (t TableConfig) WithTableName(name string) TableConfig {
	t.DBName = DBName{Name: name}
	return t
}

func (t TableConfig) AddColumns(columns ...ColumnConfig) TableConfig {
	t.Columns.AddColumns(columns...)
	return t
}

func (t TableConfig) WalkColumn(walkColumnFn func(columnConfig ColumnConfig) ColumnConfig) TableConfig {
	t.Columns.WalkColumn(walkColumnFn)
	return t
}

func (t TableConfig) AddIndexs(indexs ...Index) TableConfig {
	t.Indexs.Append(t.Columns, indexs...)
	return t
}

func (t TableConfig) WithHandler(handler Handler) TableConfig {
	t.handler = handler // 此处需要兼容事务句柄设置，不可影响已有的handler设置,所以不能使用地址引用方式覆盖，而是返回一个新的TableConfig实例

	return t
}

func (t TableConfig) GetHandler() (handler Handler) {
	if t.handler == nil {
		err := errors.New("database handler is nil, please use TableConfig.WithHandler to set handler")
		panic(err)
	}
	return t.handler
}
func (t TableConfig) GetDBNameByFieldNameMust(fieldName string) (dbName string) {
	col := t.Columns.GetByFieldNameMust(fieldName)
	return col.DbName
}
func (t TableConfig) GetDBNameByFieldName(fieldName string) (dbName string) {
	col, _ := t.Columns.GetByFieldName(fieldName)
	return col.DbName
}

func (t TableConfig) MergeTableLevelFields(ctx context.Context, fs ...*Field) Fields {
	fs1 := Fields(fs) //修改类型
	if t.TableLevelFieldsHook != nil {
		moreFields := t.TableLevelFieldsHook(ctx, fs1...)
		fs1.Append(moreFields...)
	}
	return fs1
}

var Error_UniqueIndexAlreadyExist = errors.New("unique index already exist")

func (t TableConfig) CheckUniqueIndex(allFields ...*Field) (err error) {
	indexs := t.Indexs.GetUnique()
	for _, index := range indexs {
		uFs := index.Fields(t.Columns, allFields).AppendWhereValueFn(ValueFnForward) // 变成查询条件
		columnNames := index.GetColumnNames(t.Columns)
		if len(uFs) != len(columnNames) { // 如果唯一标识字段数量和筛选条件字段数量不一致，则忽略该唯一索引校验（如 update 时不涉及到指定唯一索引）
			continue
		}
		exists, err := NewExistsBuilder(t).WithHandler(t.handler).AppendFields(uFs...).Exists()
		if err != nil {
			return err
		}
		if exists {
			data, _ := uFs.Data()
			b, _ := json.Marshal(data)
			s := string(b)
			err := errors.WithMessagef(Error_UniqueIndexAlreadyExist, "table:%s,value%s ", t.Name, s)
			return err
		}
	}
	return nil
}

//Deprecated: use GetDBNameByFieldName instead

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

// join 是Field设置时会固定表名，builder 时会设置表别名，所以需要保留最早table 的表名，后面同表名的别名
// Merge 合并表配置信息,同名覆盖，别名同名覆盖,a.Merge(b) 实现b覆盖a; b.Merge(a)、a.Merge(b,a) 可实现a 覆盖b,t.Name 不能覆盖，并且合并的table只有和t.Name 相同才会被合并，否则忽略合并操作

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

type ColumnConfig struct {
	FieldName string     // 业务标识 和Field.Name 保持一致，用户 column 和Field 互转
	DbName    string     `json:"dbName"` // 数据库字段名，和数据库字段保持一致
	Type      SchemaType `json:"type"`
	Length    int        `json:"length"`
	PK        bool       `json:"pk"`
	Unique    bool       `json:"unique"`
	Nullable  bool       `json:"nullable"`
	Default   any        `json:"default"`
	Comment   string     `json:"comment"`
	Enums     Enums      `json:"enums"`
}

// Deprecated: use NewColumn instead
func NewColumnConfig(dbName, fieldName string) ColumnConfig {
	return ColumnConfig{
		FieldName: fieldName,
		DbName:    dbName,
	}
}

// newColumnConfig 内部使用
func newColumnConfig(dbName, fieldName string) ColumnConfig {
	return ColumnConfig{
		FieldName: fieldName,
		DbName:    dbName,
	}
}

// NewColumn 新建列配置，用于封装模型时，将字段映射为数据库字段,使用*Field作为参数，能减少硬编码，减少硬编码带来的维护成本
func NewColumn(dbFieldName string, field *Field) ColumnConfig {
	return newColumnConfig(dbFieldName, field.Name)
}

// DbNameIsEmpty 字段DbName 为空，对数据表操作来说，是无效字段，但是业务层面封装的模型支持退化，比如keyvalue 模型正常2个字段，但是只保留key 也是常见模型，这是将value映射为""，即可实现key模型，于是诞生 了DbName 为空的数据，此函数协助过滤
func (c ColumnConfig) DbNameIsEmpty() bool {
	return c.DbName == ""
}

func (c ColumnConfig) CamelName() string {
	return funcs.CamelCase(c.DbName, false, false)
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

func (cs *ColumnConfigs) AddColumns(cols ...ColumnConfig) {
	if *cs == nil {
		*cs = make([]ColumnConfig, 0)

	}
	*cs = append(*cs, cols...)
	//2025-06-18 09:50 注释去重，将 数据库字段和业务字段改为1:N关系，理由：
	// 1. 业务上存在多个字段对应数据库一个字段，比如id,ids
	// 2. 改成1:N 关系后,提前封装的业务模型内数据表字段映射不会影响实际业务字段表映射的完整性，相当于站在各自的领域角度，操作同一个字段，能有效解耦提前封装的业务模块和实际扩展的模型
	// 3. 改成1:N 关系后,基本没有副作用，在通过Columns 生成ddl时可以根据dbName 去重即可
	//cs.UniqueueByFieldName() //根据FieldName 去重,同名覆盖，由于预先写模型时，fieldName 是固定的，dbName是后期根据业务定义的，所以这里支持fieldName覆盖
}

// Uniqueue 去重,同名覆盖（保留最后设置）,由于预先写模型时，fieldName 是固定的，dbName是后期根据业务定义的，所以这里支持fieldName覆盖
func (cs *ColumnConfigs) UniqueueByFieldName() *ColumnConfigs {
	m := make(map[string]ColumnConfig)
	arr := make([]ColumnConfig, 0)
	for i := len(*cs) - 1; i >= 0; i-- {
		if _, exists := m[(*cs)[i].FieldName]; !exists {
			arr = append(arr, (*cs)[i])
			m[(*cs)[i].FieldName] = (*cs)[i]
		}
	}
	slices.Reverse(arr)
	*cs = arr
	return cs
}

func (cs ColumnConfigs) WalkColumn(walkFn func(columnConfig ColumnConfig) ColumnConfig) {
	if walkFn == nil {
		return
	}
	for i := range cs {
		(cs)[i] = walkFn((cs)[i])
	}
}

func (cs ColumnConfigs) Merge(others ...ColumnConfig) ColumnConfigs {
	//cs = append(cs, others...)
	cs.AddColumns(others...) // 这里使用AddColumns，是为了支持同名覆盖
	return cs
}

// GetByFieldName  通过标识获取列配置信息，找不到则panic退出。主要用于生成字段时快速定位列配置信息。
func (cs ColumnConfigs) GetByFieldNameMust(fieldName string) (c ColumnConfig) {
	c, exists := cs.GetByFieldName(fieldName)
	if !exists {
		err := errors.Errorf("ColumnConfig not found by fieldName: " + string(fieldName))
		panic(err)
	}
	return c
}

func (cs ColumnConfigs) GetByDbNameMust(dbName string) (c ColumnConfig) {
	c, exists := cs.GetByDbName(dbName)
	if !exists {
		err := errors.Errorf("ColumnConfig not found by dbName: " + string(dbName))
		panic(err)
	}
	return c
}

func (cs ColumnConfigs) FieldName2ColumnName(fieldNames ...string) (columnNames []string) {
	columnNames = make([]string, len(fieldNames))

	for i, fieldName := range fieldNames {
		c := cs.GetByFieldNameMust(fieldName)
		columnNames[i] = c.DbName
	}
	return columnNames
}

func (cs ColumnConfigs) GetByFieldName(fieldName string) (c ColumnConfig, exists bool) {
	for _, c := range cs {
		if strings.EqualFold(c.FieldName, fieldName) {
			return c, true
		}
	}
	return c, false
}
func (cs ColumnConfigs) GetByDbName(dbName string) (c ColumnConfig, exists bool) {
	for _, c := range cs {
		if strings.EqualFold(c.DbName, dbName) {
			return c, true
		}
	}
	return c, false
}

func (cs ColumnConfigs) FilterByFieldName(fieldNames ...string) (result ColumnConfigs) {
	for _, c := range cs {
		if slices.Contains(fieldNames, c.FieldName) {
			result.AddColumns(c)
		}
	}
	return result
}

func (cs ColumnConfigs) FilterByEmptyDbName(fieldNames ...string) (result ColumnConfigs) {
	for _, c := range cs {
		if c.DbName != "" {
			result.AddColumns(c)
		}
	}
	return result
}

type AliasedExpressions []exp.AliasedExpression

// AsAny 转成任意类型，主要用于Field.SetSelectColumns
func (a AliasedExpressions) AsAny() []any {
	return Slice2Any(a)
}

func (cs ColumnConfigs) DbNameWithAlias() AliasedExpressions {
	var result AliasedExpressions
	for _, c := range cs {
		result = append(result, goqu.I(c.DbName).As(c.FieldName))
	}
	return result
}

func Slice2Any[T any](arr []T) (out []any) {
	if arr == nil {
		return nil
	}
	out = make([]any, len(arr))
	for i, v := range arr {
		out[i] = v
	}
	return out
}
