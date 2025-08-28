package sqlbuilder

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/memorytable"
)

type Layer string

func (l Layer) String() string {
	return string(l)
}
func (l Layer) EqualFold(layer Layer) bool {
	return strings.EqualFold(l.String(), layer.String())
}
func (l Layer) IsEmpty() bool {
	return l.String() == ""
}

const (
	Value_Layer_SetValue    Layer = "SetValue"    //赋值层
	Value_Layer_SetFormat   Layer = "setFormat"   //赋值后格式化层，用于重置或者解码等场景
	Value_Layer_ApiValidate Layer = "ApiValidate" //验证层
	Value_Layer_ApiFormat   Layer = "ApiFormat"   //解码前格式化层
	Value_Layer_DBValidate  Layer = "DBValidate"  //DB验证层
	Value_Layer_DBFormat    Layer = "DBFormat"    //格式化层
	Value_Layer_OnlyForData Layer = "OnlyForData" //只用于 insert values，update  set 部分，不用于where部分

)

type Phase = Layer

// todo 后去重构
const (
	//单字段步骤定义
	PhaseField_ApiSetValue = Value_Layer_SetValue    // api赋值层
	PhaseField_ApiValidate = Value_Layer_ApiValidate // api验证层
	PhaseField_ApiFormat   = Value_Layer_ApiFormat   // api格式化层
	PhaseFields_ApiFormat  = "api_fields_format"
	PhaseField_DBFormat    = Value_Layer_DBFormat // DB格式化层(scene insert,update,select 等场景归纳到DBFormat层，便于统一处理)

	PhaseField_DBValidate = Value_Layer_DBValidate // DB验证层
	//多字段步骤定义
	PhaseFields_DBFormat = "api_fields_modify" // 修正所有列的时机  取代 CustomFieldsFn ValueFnFn func(_ any, f *Field, fs ...*Field),此时重点在于修正fs

)

var (
	//todo layer 改成 Scope 作用域
	//layer_order 确保层序,越靠前越先执行
	layer_order               = []Layer{Value_Layer_SetValue, Value_Layer_SetFormat, Value_Layer_ApiValidate, Value_Layer_ApiFormat, Value_Layer_DBValidate, Value_Layer_DBFormat, Value_Layer_OnlyForData} // 层序,越靠前越先执行
	Layer_all                 = layer_order
	Layer_where               = []Layer{Value_Layer_SetValue, Value_Layer_SetFormat, Value_Layer_ApiValidate, Value_Layer_ApiFormat, Value_Layer_DBValidate, Value_Layer_DBFormat} // where  场景下执行的函数
	Layer_get_value_before_db = []Layer{Value_Layer_SetValue, Value_Layer_SetFormat, Value_Layer_ApiValidate, Value_Layer_ApiFormat}                                               // 获取转换成db数据格式之前的原始数据
	Layer_Validate            = []Layer{Value_Layer_SetValue, Value_Layer_SetFormat, Value_Layer_ApiValidate, Value_Layer_ApiFormat, Value_Layer_DBValidate}                       // 验证数据时执行的函数

)

type ValueFnFn func(inputValue any, f *Field, fs ...*Field) (any, error)
type ValueFn struct {
	Name        string
	Fn          ValueFnFn
	Order       int    // 执行顺序，越小越先执行
	Layer       Layer  //Deprecated: 废弃，使用Tags代替
	Phase       string // 阶段，指定函数在哪个阶段执行，比如：builder-构建阶段执行,running-传入数据后执行
	Description string // 描述
}

func (fn ValueFn) IsEqual(v ValueFn) bool {
	return fn.Name != "" && fn.Name == v.Name && fn.Layer.EqualFold(v.Layer)
}

func (fn ValueFn) IsNil() bool {
	return fn.Fn == nil
}
func (fn ValueFn) IsSetValueFn() bool {
	return fn.Layer.EqualFold(Value_Layer_SetValue)
}

func (vs *ValueFns) Append(value ...ValueFn) *ValueFns {
	if *vs == nil {
		*vs = make(ValueFns, 0)
	}
	*vs = append(*vs, value...)
	return vs
}

func (values ValueFns) GetByLayer(layers ...Layer) (subValues ValueFns) {
	subValues = make(ValueFns, 0)
	for _, v := range values {
		for _, l := range layers {
			if strings.EqualFold(v.Layer.String(), l.String()) {
				subValues = append(subValues, v)
			}
		}

	}
	return subValues
}

func (values ValueFns) HasSetValueLayer() bool {
	return len(values.GetByLayer(Value_Layer_SetValue)) > 0
}

func (values ValueFns) Value(val any, f *Field, fs ...*Field) (value any, err error) {
	if !values.HasSetValueLayer() {
		return nil, nil
	}
	value = val
	for _, layer := range layer_order {
		subValues := values.GetByLayer(layer)
		subValues.Sort() // 增加排序,确保执行顺序正确
		for _, v := range subValues {
			if v.IsNil() {
				continue
			}

			cf := f.Copy()
			cfs := Fields(fs).Copy()
			value, err = v.Fn(value, cf, cfs...) //格式化值,后面2个参数移除当前的字段,避免循环调用
			if err != nil {
				return value, err
			}
		}
	}
	return value, nil
}

// type ValueFnfunc(in any) (value any,err error) //函数签名返回参数命名后,容易误导写成 func(in any) (value any,err error){return value,nil};  正确代码:func(in any) (value any,err error){return in,nil};
//type ValueFn func(inputValue any) (any, error) // 函数之所有接收in 入参，有时模型内部加工生成的数据需要存储，需要定制格式化，比如多边形产生的边界框4个点坐标

type ValueFns []ValueFn

func (vFns ValueFns) Sort() {
	sort.SliceStable(vFns, func(i, j int) bool {
		return vFns[i].Order < vFns[j].Order
	})
}

func (fns *ValueFns) Reset(subFns ...ValueFn) {
	*fns = make(ValueFns, 0)
	*fns = append(*fns, subFns...)
}

func (fns *ValueFns) Remove(v ValueFn) {
	tmp := ValueFns{}
	for _, fn := range *fns {
		if fn.IsEqual(v) {
			continue
		}
		tmp = append(tmp, fn)
	}
	*fns = tmp
}

// ResetSetValueFn 重置设置值类型函数
func (fns *ValueFns) ResetSetValueFn(setValueFnFns ...ValueFnFn) {
	tmp := make(ValueFns, 0)
	for _, setValueFnFn := range setValueFnFns {
		setValueFn := ValueFn{
			Fn:    setValueFnFn,
			Layer: Value_Layer_SetValue,
		}
		tmp = append(tmp, setValueFn)
	}
	for _, v := range *fns {
		if v.IsSetValueFn() {
			continue
		}
		tmp = append(tmp, v)
	}

	*fns = tmp
}

func (vs ValueFns) Filter(fn func(fn ValueFn) bool) (subFns ValueFns) {
	for _, v := range vs {
		if fn(v) {
			subFns = append(subFns, v)
		}
	}
	return subFns
}

var ValueFnWhereLike = ValueFnWhereLikev2(true, true)

func ValueFnWhereLikev2(left, right bool) ValueFn {
	likeLeft := ""
	if left {
		likeLeft = "%"
	}
	likeRight := ""
	if right {
		likeRight = "%"
	}
	valueFn := ValueFn{
		Fn: func(val any, f *Field, fs ...*Field) (value any, err error) {
			if IsNil(val) {
				return val, nil
			}
			str := cast.ToString(val)
			if str == "" {
				return val, nil
			}
			value = Ilike{likeLeft, str, likeRight}

			return value, nil
		},
		Layer: Value_Layer_DBFormat,
	}
	return valueFn

}

var OrderFnDesc OrderFn = func(f *Field, fs ...*Field) (orderedExpressions []exp.OrderedExpression) {
	return ConcatOrderedExpression(goqu.I(f.DBColumnName().FullName()).Desc())
}
var OrderFnAsc OrderFn = func(f *Field, fs ...*Field) (orderedExpressions []exp.OrderedExpression) {
	return ConcatOrderedExpression(goqu.I(f.DBColumnName().FullName()).Asc())
}

// OrderFieldFn 给定列按指定值顺序排序
func OrderFieldFn(valueOrder ...any) OrderFn {
	return func(f *Field, fs ...*Field) (orderedExpressions []exp.OrderedExpression) {
		segment := fmt.Sprintf("FIELD(%s %s)", f.DBColumnName().FullNameWithQuotes(), strings.Repeat(",?", len(valueOrder))) // 此处 f.DBName() 可能返回 table.column 格式，所以不加 ``
		expression := goqu.L(segment, valueOrder...)
		orderedExpression := exp.NewOrderedExpression(expression, exp.AscDir, exp.NoNullsSortType)
		orderedExpressions = ConcatOrderedExpression(orderedExpression)
		return orderedExpressions
	}
}

type OrderFn func(f *Field, fs ...*Field) (orderedExpressions []exp.OrderedExpression)

type OrderFnWithSort struct {
	Sort int
	Fn   OrderFn
}

type OrderFnWithSorts []OrderFnWithSort

type OnUnit struct {
	Table       TableConfig
	Field       *Field
	WhereFields Fields
}

func (onUnit OnUnit) moreCondition() (moreWhereCondition Expressions) {
	if onUnit.WhereFields == nil {
		return nil
	}
	moreWhereCondition, err := onUnit.WhereFields.Where()
	if err != nil { // on 中的条件只用于指定表连接的条件,不做数据筛选（需要动态筛选数据必须使用where条件）;所以 这里的更多条件的值是固定的 这里如果出错了，是必然会报错的，直接panic即可,方便使用
		err = errors.Errorf("moreCondition error: %v", err)
		panic(err)
	}
	return moreWhereCondition
}

type _On [2]OnUnit

func NewOn(first, second OnUnit) _On {
	on := _On{first, second}
	on.init()
	return on
}

func (on _On) init() {
	//初始化表关系
	for i := 0; i < len(on); i++ {
		on[i].Field.SetTable(on[i].Table)
		if on[i].WhereFields != nil {
			on[i].WhereFields.SetTable(on[i].Table)
		}
	}
}

func (on _On) moreCondition() (moreWhereCondition Expressions) {
	for i := 0; i < len(on); i++ {
		condition := on[i].moreCondition()
		moreWhereCondition = append(moreWhereCondition, condition...)
	}
	return moreWhereCondition
}

func (on _On) Table() exp.Expression {
	return on[1].Table.AliasOrTableExpr()
}
func (on _On) Condition() (joinTable exp.Expression, condition exp.JoinCondition) {
	first, second := on[0], on[1]
	expressions := make([]exp.Expression, 0)
	expression := goqu.I(first.Field.DBColumnName().FullName()).Eq(goqu.I(second.Field.DBColumnName().FullName()))
	expressions = append(expressions, expression)
	moreCondition := on.moreCondition()
	expressions = append(expressions, moreCondition...)
	condition = goqu.On(expressions...)
	table := on.Table()
	return table, condition
}

func Join(ds *goqu.SelectDataset, jionConfigs ...OnUnit) *goqu.SelectDataset {
	return ds
}

// Field 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Field struct {
	Name  string `json:"name"`
	Value any    //增加原始值记录，和ValueFns 脱钩，后续ValueFns/WhereFns 合并使用pipeLine模式,对原始value 加工

	//todo 后续迁移到tags(tag 分组名称为 value)
	ValueFns ValueFns `json:"-"` // 增加error，方便封装字段验证规则
	// 当值作为where条件时，调用该字段格式化值，该字段为nil则不作为where条件查询,没有error，验证需要在ValueFn 中进行,数组支持中间件添加转换函数，转换函数在field.GetWhereValue 统一执行
	//todo 后续迁移到tags(tag 分组名称为 where)
	WhereFns ValueFns `json:"-"`
	//_OrderFn          OrderFn         `json:"-"` //deprecated  排序函数
	_OrderFnWithSort OrderFnWithSort `json:"-"` // 排序函数,支持多个排序规则

	Schema *Schema     // 可以为空，为空建议设置默认值
	table  TableConfig // 关联表,方便收集Table全量信息
	scene  Scene       // 场景
	//todo 后续迁移到tags（tag 分组名称为 scene）
	sceneFns      SceneFns // 场景初始化配置
	tags          Tags     // 方便搜索到指定列,Name 可能会更改,tag不会,多个tag,拼接,以,开头
	dbName        string   //Deprecated 废弃，使用DBColumnName代替
	docName       string   //Deprecated 废弃，使用DBColumnName代替
	selectColumns []any    // 查询时列
	fieldName     string   //列名称,方便通过列名称找到列,列名称根据业务取名,比如NewDeletedAtField 取名 deletedAt
	alias         Fields   // 别名字段,方便在查询时使用别名做为字段名
	//todo 后续迁移到tags(tag 分组名称为 stage)
	delayApplies ApplyFns // 延迟执行函数 在 xxx.ToSQL()中调用，在执行后才执行中间件(如在设置f.SetSelectColumns 时需要获取 f.Table().Columns 信息时，就需要延迟执行中间件)
	//ddlSequence   int         // 生成ddl语句时排序字段，一般不用，在多字段联合唯一索引/主键 时 将多字段值拼接时会使用到

	//indexs        Indexs // 索引(索引跟表走，不在领域语言上)
	//applyFns      ApplyFns // apply 必须当场执行，因为存在apply函数嵌套apply函数,
}

func (f Field) Fields() Fields {
	return Fields{&f}
}

// MakeDBColumnWithAlias 生成数据库列别名，方便在查询时使用别名做为字段名,比如模块封装，基本都会用到提前设置好的别名
func (f Field) MakeDBColumnWithAlias(tableColumns ColumnConfigs) any {
	col := tableColumns.GetByFieldNameMust(f.Name)
	alias := goqu.I(col.DbName).As(f.Name)
	return alias
}

func (f *Field) AddAlias(fs ...*Field) *Field {
	f.alias = append(f.alias, fs...)
	return f
}
func (f Field) GetAlias() Fields {
	return f.alias
}

type Index struct {
	IsPrimary bool `json:"isPrimary"` // 是否主键索引
	Unique    bool `json:"unique"`    // 是否唯一索引
	//Name        string   `json:"name"`   // 索引名称
	ColumnNames func(tableColumns ColumnConfigs) (columnNames []string) //在实际封装模块时,已知 Field.Name ,DB.Column.Name 未知，需要支持通过 Field.Name 转DB.Column.Name,所以设计成函数格式
	Weight      int                                                     `json:"weight"` // 索引权重,越大的表明越重要，能优先作为记录的唯一标识（程序自动识别记录唯一标识时会用到）

}

func (i Index) GetColumnNames(tableColumns ColumnConfigs) []string {
	if i.ColumnNames == nil {
		err := errors.Errorf("Index.ColumnNames is nil")
		panic(err)
	}
	columnNames := i.ColumnNames(tableColumns)
	return columnNames
}

func (i Index) IndexName(tableColumns ColumnConfigs) string {
	prefix := "idx"
	if i.Unique {
		prefix = "uniq"
	}
	arr := append([]string{prefix}, i.GetColumnNames(tableColumns)...)
	indexName := strings.Join(arr, "_")
	return indexName

}

func (i Index) Fields(tableColumns ColumnConfigs, allFields Fields) (fields Fields) {
	alldbCoumns := i.GetColumnNames(tableColumns)
	for _, field := range allFields {
		dbName := field.DBColumnName().BaseName()
		ok := slices.Contains(alldbCoumns, dbName)
		if ok {
			fields = append(fields, field)
		}

	}

	return fields
}

type Indexs []Index

func (indexs *Indexs) Append(tableColumns ColumnConfigs, subIndexs ...Index) {
	if *indexs == nil {
		*indexs = make(Indexs, 0)
	}
	for _, index := range subIndexs {
		if indexs.HasIndex(index, tableColumns) {
			continue
		}
		*indexs = append(*indexs, index)
	}
}
func (indexs Indexs) HasIndex(index Index, tableColumns ColumnConfigs) bool {
	for _, i := range indexs {
		if index.IndexName(tableColumns) == i.IndexName(tableColumns) && index.Unique == i.Unique {
			return true
		}
	}
	return false
}

func (indexs Indexs) GetUnique() (uniqueIndex Indexs) {
	uniqueIndex = make(Indexs, 0)
	for _, index := range indexs {
		if index.Unique {
			uniqueIndex = append(uniqueIndex, index)
		}
	}
	return uniqueIndex
}

func (indexs Indexs) GetPrimary() (primary *Index, exists bool) {
	for _, index := range indexs {
		if index.IsPrimary {
			return &index, true
		}
	}
	return nil, false
}

func (indexs Indexs) First() (index *Index, exists bool) {
	if len(indexs) == 0 {
		return nil, false
	}
	return &indexs[0], true
}

// SortByColCount  根据列数量排序，优先使用唯一索引，其次使用主键索引（有唯一索引情况下，主键id往往是没有业务含义的列），最后使用复合索引,这个主要用于寻找对象唯一标识
func (indexs Indexs) SortByColCount(tableColumns ColumnConfigs) Indexs {
	slices.SortStableFunc(indexs, func(i, j Index) int {
		diff := i.Weight - j.Weight
		if diff != 0 {
			diff = diff * -1 // 权重大的排在前面
			return diff
		}
		iColumCount := len(i.GetColumnNames(tableColumns))
		jColumCount := len(j.GetColumnNames(tableColumns))
		iOrdinary := i.Unique && !j.Unique
		jOrdinary := !j.Unique && !j.IsPrimary

		//1. i,j 索引类型相同(同时为唯一索引、主键、普通索引)，比较列数量
		if (i.Unique && j.Unique) || (i.IsPrimary && j.IsPrimary) || (iOrdinary && jOrdinary) {
			diff := iColumCount - jColumCount
			return diff
		}
		//2. i,j 索引类型不同
		//2.1 i唯一索引，j通索引，i排在前面
		if i.Unique && jOrdinary {
			return -1
		}
		//2.2 i唯一索引，j主键，列长度小的排前面，列长度相等，i排在前面
		if i.Unique && j.IsPrimary {
			diff := iColumCount - jColumCount
			if diff == 0 {
				return -1
			}
			return diff
		}
		//2.3 i主键，j通索引，i排在前面
		if i.IsPrimary && jOrdinary {
			return -1
		}
		//2.4 i主键，j唯一索引，列长度小的排前面，列长度相等，j排在前面
		if i.IsPrimary && j.Unique {
			diff := iColumCount - jColumCount
			if diff == 0 {
				return 1
			}
			return diff
		}
		//2.5 i通索引，j唯一索引或者主键索引，j排在前面
		if iOrdinary && (j.Unique || j.IsPrimary) {
			return 1
		}
		//2.6 i通索引，j通索引，列长度小的排前面
		diff = iColumCount - jColumCount
		return diff
	})
	return indexs
}

type Tags []string

func (tags Tags) HastTag(tag string) bool {
	for _, t := range tags {
		if strings.EqualFold(t, tag) {
			return true
		}
	}
	return false
}
func (ts *Tags) Append(tags ...string) bool {
	if *ts == nil {
		*ts = make(Tags, 0)
	}
	for _, tag := range tags {
		if ts.HastTag(tag) {
			continue
		}
		*ts = append(*ts, tag)
	}
	return false
}

const (
	Field_name_id        = "id"        // 列取名为id
	Field_name_deletedAt = "deletedAt" // 列取名为deletedAt 为删除列
)

const (
	Field_tag_pageIndex = "pageIndex" // 标记为pageIndex列
	Field_tag_pageSize  = "pageSize"  //标记为pageSize列
	//Field_tag_update_limit = "updateLimit" // 标记为updateLimit列 ,使用PageSize标记，减少比必要预先定义

	Field_tag_CanWriteWhenDeleted = "CanWriteWhenDeleted" // 标记为删除场景下，可以更新数据库字段（如操作人 ，Field_name_deletedAt 自带该标签功能）
)

// 不复制whereFns，ValueFns
func (f *Field) Copy() (copyF *Field) {
	fcp := *f
	if f.Schema != nil { // schema 为地址引用，需要单独复制
		fcp.Schema = f.Schema.Copy()
	}
	tags := f.tags
	fcp.tags = tags
	// indexs := f.indexs
	// fcp.indexs = indexs
	return &fcp
}

// func (f *Field) SetDDLsequence(DDLsequence int) *Field {
// 	f.ddlSequence = DDLsequence
// 	return f
// }

const (
	Tag_createdAt     = "createdAt"
	Tag_updatedAt     = "updatedAt"
	Tag_datetime      = "datetime"
	Tag_autoIncrement = "autoIncrement"
	Tag_unsigned      = "unsigned"
)

// func (f *Field) AddIndex(indexs ...Index) *Field {
// 	f.indexs.Append(indexs...)
// 	return f
// }

// Deprecated: Use WithOrderFn instead.
func (f *Field) SetOrderFn(orderFn OrderFn) *Field {
	return f.WithOrderFn(0, orderFn)
}
func (f *Field) WithOrderFn(sort int, orderFn OrderFn) *Field {
	f._OrderFnWithSort = OrderFnWithSort{
		Sort: sort,
		Fn:   orderFn,
	}
	return f
}

// SetTable 设置表配置信息，不存在则设置,存在则合并,合并策略: Field.Table 优先级最高,最早的Table.Name为基准表名
func (f *Field) SetTable(table TableConfig) *Field {
	f.table = f.table.Merge(table)
	return f
}

func (f *Field) GetTable() (table TableConfig) {
	return f.table
}

func (f *Field) GetScene() (scena Scene) {
	return f.scene
}

func (f *Field) JoinConfig() (joinConfig OnUnit) {
	return OnUnit{
		Table: f.table,
		Field: f,
	}
}

// ReadOnly 很多字段只能写入一次，即新增写入后不可更改，如记录的所有者，指纹等，此处方便理解 重写f.ShieldUpdate(true)
func (f *Field) ReadOnly() *Field {
	f.ShieldUpdate(true)
	return f
}
func (f *Field) CanUpdate(condition bool) *Field {
	f.ShieldUpdate(!condition)
	return f
}

type DBColumnName struct {
	DBName
	Table TableConfig
}

func (dbColName DBColumnName) FullName() string { //构建where条件部分，使用fullname 能兼容不连表、连表不取别名、连表取别名情况
	identifier := DBIdentifier{
		dbColName.Table.Schema.DBName,
		dbColName.Table.DBName,
		dbColName.DBName,
	}
	return identifier.FullName()
}
func (dbColName DBColumnName) BaseName() string {
	identifier := DBIdentifier{
		dbColName.Table.Schema.DBName,
		dbColName.Table.DBName,
		dbColName.DBName,
	}
	return identifier.BaseName()
}

func (dbColName DBColumnName) FullNameWithQuotes() string {
	identifier := DBIdentifier{
		dbColName.Table.Schema.DBName,
		dbColName.Table.DBName,
		dbColName.DBName,
	}
	return identifier.FullNameWithQuotes()
}

func (f *Field) DBColumnName() (dbName DBColumnName) {
	arr := strings.Split(f._DBName(), ".")
	switch len(arr) {
	case 1:
		return DBColumnName{
			DBName: DBName{
				Name: arr[0],
			},
			Table: f.table,
		}
	case 2:
		table := f.table
		table.DBName.Name = arr[0]
		return DBColumnName{
			DBName: DBName{
				Name: arr[1],
			},
			Table: table,
		}
	case 3:
		table := f.table
		table.DBName.Name = arr[1]
		table.Schema.Name = arr[0]
		return DBColumnName{
			DBName: DBName{
				Name: arr[2],
			},
			Table: table,
		}

	}

	return DBColumnName{
		DBName: DBName{
			Name: f._DBName(),
		},
		Table: f.table,
	}
}

var Use_Filed_column_map = false // 开启后，只能使用映射字段名，目前为了兼容性，默认关闭，后续会默认开启

// _DBName 转换为DB字段,此处增加该,方法方便跨字段设置(如 polygon 设置外接四边形,使用Between)
func (f *Field) _DBName() (dbName string) { // 改为私有方法，外部使用DBColumnName().Fullname()
	if Use_Filed_column_map {
		return f.table.GetDBNameByFieldNameMust(f.Name)
	}
	if f.dbName != "" { // 后续要废弃dbName字段，使用 f.table.GetDBNameByFieldName(f.Name) 获取
		return f.dbName
	}
	dbName = f.table.GetDBNameByFieldName(f.Name) // 后续仅保留这种方式转换为DB字段名,方便将Field 和数据表解耦
	//使用自带函数转换
	if dbName == "" && f.table.FieldName2DBColumnNameFn != nil { // 存在dbName则使用dbName
		dbName = f.table.FieldName2DBColumnNameFn(f.Name)
	}
	if dbName == "" { // 使用全局函数转换
		dbName = FieldName2DBColumnName(f.Name)
	}
	return dbName
}

// DBName 转换为DB字段,此处增加该,方法方便跨字段设置(如 polygon 设置外接四边形,使用Between)
func (f *Field) SetDBName(dbName string) *Field {
	f.dbName = dbName
	return f
}

/* 这个方法有bug
func ColumnToString(col any) string {
	s := ""
	switch v := col.(type) {
	case string:
		s = v
	case exp.AliasedExpression:
		s = identifierExpression2String(v.GetAs())
	case exp.IdentifierExpression:
		s = identifierExpression2String(v)
	default:
		s = cast.ToString(v)
	}

	return s
}
*/

// func identifierExpression2String(v exp.IdentifierExpression) string {
// 	var w bytes.Buffer
// 	schema := v.GetSchema()
// 	if schema != "" {
// 		w.WriteString(schema)
// 		w.WriteString(".")
// 	}
// 	table := v.GetTable()
// 	if table != "" {
// 		w.WriteString(table)
// 		w.WriteString(".")
// 	}
// 	col := v.GetCol()
// 	if col != "" {
// 		w.WriteString(cast.ToString(col))
// 	}
// 	s := w.String()
// 	return s
// }

func (f *Field) SetSelectColumns(columns ...any) *Field {
	//colMap := make(map[any]struct{}, 0)// 并非所有类型都可以作为map的key(runtime error: hash of unhashable type exp.sqlFunctionExpression)，此处使用string 作为key 更安全
	colMap := make(map[string]struct{}, 0)
	for _, col := range columns { // 保持稳定顺序
		if str, ok := col.(string); ok && str == "" { // 删除空字符串字段，避免错误（如未使用 ColumnConfig.FilterByEmptyDbName 过滤场景）
			continue
		}
		key := fmt.Sprint(col)
		if _, ok := colMap[key]; !ok { // 去重
			colMap[key] = struct{}{}
			f.selectColumns = append(f.selectColumns, col)
		}
	}
	return f
}

func (f *Field) Select() (columns []any) {
	return f.selectColumns
}

func (f *Field) SetName(name string) *Field {
	if name != "" {
		f.Name = name
	}
	return f
}

func (f *Field) SetFieldName(fieldName string) *Field {
	if fieldName != "" {
		f.fieldName = fieldName
	}
	return f
}

func (f *Field) GetFieldName() string {
	return f.fieldName
}

func (f *Field) SetTitle(title string) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Title = title
	return f
}

func (f *Field) SetType(typ SchemaType) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Type = typ
	return f
}
func (f *Field) SetFormat(format string) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Format = format
	return f
}

func (f *Field) SetBaseInfo(name string, typ SchemaType, title string) *Field {
	f.SetName(name).SetType(typ).SetTitle(title)

	return f
}

// Comment 设置注释 针对DDL 语义化
func (f *Field) Comment(comment string) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Comment = comment
	return f
}
func (f *Field) SetLength(maxLength int) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.MaxLength = maxLength
	return f
}
func (f *Field) SetMaximum(maximum uint) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Maximum = maximum
	return f
}
func (f *Field) SetMinimum(minimum int) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Minimum = &minimum
	return f
}

// SetDescription 设置描述 针对api 语义化
func (f *Field) SetDescription(description string) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Comment = description
	return f
}
func (f *Field) SetDefault(defaul any) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Default = defaul
	return f
}
func (f *Field) SetTag(tag string) *Field {
	if len(f.tags) == 0 {
		f.tags = append(f.tags, tag)
	}
	return f
}

func (f *Field) HastTag(tag string) bool {
	if len(f.tags) == 0 {
		return false
	}
	return f.tags.HastTag(tag)
}

// func (f *Field) HasIndex(index Index) bool {
// 	if len(f.indexs) == 0 {
// 		return false
// 	}
// 	return f.indexs.HasIndex(index)
// }
// func (f *Field) GetIndexs() Indexs {
// 	return f.indexs
// }

func (f *Field) GetTags() Tags {
	return f.tags
}
func (f *Field) AppendEnum(enums ...Enum) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Enums = append(f.Schema.Enums, enums...)
	for _, enum := range enums {
		if enum.IsDefault {
			f.Schema.Default = enum.Key
		}
	}
	return f
}
func (f *Field) SetRequired(required bool) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Required = required // 此处 requied 可以为false,通过MergerSchema 达不到效果
	return f
}
func (f *Field) IsRequired() bool {
	return f.Schema != nil && f.Schema.Required
}
func (f *Field) SetAllowZero(zeroAsEmpty bool) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.SetAllowZero(zeroAsEmpty)
	return f
}

func (f *Field) AppendValueFn(valueFns ...ValueFn) *Field {
	f.ValueFns.Append(valueFns...)
	return f
}

// Subscribe 专注解决当前字段值依赖其他字段值转换生成场景
func (f *Field) Subscribe(subFn func(srcValues ...any) (value any, err error), dependentFs ...*Field) *Field {
	f.SceneSave(func(f *Field, fs ...*Field) {
		f.ValueFns.ResetSetValueFn(func(inputValue any, f *Field, fs ...*Field) (any, error) {
			if subFn == nil {
				return nil, nil
			}
			fieldCout := len(dependentFs)
			if fieldCout == 0 {
				return nil, errors.New("dependentFs required")
			}
			srcValues := make([]any, fieldCout)
			for i, emptySrcField := range dependentFs {
				srcField, ok := Fields(fs).GetByName(emptySrcField.Name)
				if ok {
					srcValue, err := srcField.GetValue(Layer_all, fs...)
					if err != nil {
						return nil, err
					}
					srcValues[i] = srcValue
				}
			}
			value, err := subFn(srcValues...)
			if err != nil {
				return nil, err
			}
			return value, nil
		})
	})
	return f
}

func (f *Field) AppendWhereFn(whereFns ...ValueFn) *Field {
	f.WhereFns.Append(whereFns...)
	return f
}

func (f *Field) ResetValueFn(valueFns ...ValueFn) *Field {
	f.ValueFns.Reset(valueFns...)
	return f
}

func (f *Field) ResetWhereFn(whereFns ...ValueFn) *Field {
	f.WhereFns.Reset(whereFns...)
	return f
}

// RequiredWhenInsert 经常在新增时要求必填,所以单独一个函数,提供方便
func (f *Field) RequiredWhenInsert(required bool) *Field {
	f.SceneInsert(func(f *Field, fs ...*Field) {
		f.SetRequired(true)
	})
	return f
}

// MinBoundaryLengthWhereInsert 数字最小值,字符串最小长度设置,提供方便
func (f *Field) MinBoundaryWhereInsert(minimum int, minLength int) *Field {
	f.SceneInsert(func(f *Field, fs ...*Field) {
		f.MergeSchema(Schema{
			Minimum:   &minimum,
			MinLength: minLength,
		})
	})
	return f
}

func (f *Field) ShieldUpdate(shieldUpdate bool) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.ShieldUpdate = shieldUpdate
	return f
}

// Combine 混合field属性,通过Combine,可以将field 各属性分层段书写，提高复用率
func (f *Field) Combine(combinedFields ...*Field) *Field {
	schema := Schema{}
	for _, combined := range combinedFields {
		if combined == nil {
			continue
		}
		if f.Name == "" {
			f.Name = combined.Name
		}
		f.table = combined.table.Merge(f.table)
		if f.scene == "" {
			f.scene = combined.scene
		}
		if f.dbName == "" {
			f.dbName = combined.dbName
		}
		if f.docName == "" {
			f.docName = combined.docName
		}
		if len(f.selectColumns) == 0 {
			f.selectColumns = combined.selectColumns
		}
		// f.indexs.Append(combined.indexs...)
		// if f.fieldName == "" {
		// 	f.fieldName = combined.fieldName
		// }
		f.tags.Append(combined.tags...)
		f.sceneFns.Append(combined.sceneFns...)
		//	f.ValueFns.Append(combined.ValueFns...) value 不可写入
		f.WhereFns.Append(combined.WhereFns...)
		if f._OrderFnWithSort.Fn == nil {
			f._OrderFnWithSort = combined._OrderFnWithSort
		}
		if combined.Schema != nil {
			schema.Merge(*combined.Schema)
		}

	}
	if f.Schema == nil {
		f.Schema = &schema
	}
	schema.Merge(*f.Schema)
	*f.Schema = schema
	return f
}

func (f *Field) MergeSchema(schema Schema) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Merge(schema)
	return f
}

func (f *Field) SetSchema(schema Schema) *Field {
	if schema.Title == "" && f.Schema != nil {
		schema.Title = f.Schema.Title // 标题不存在则复制
	}
	f.Schema = &schema
	return f
}

// LogString 日志字符串格式
func (f *Field) LogString() string {
	title := f.Name
	if f.Schema != nil && f.Schema.Title == "" {
		title = f.Schema.Title
	}
	val, _ := f.GetValue(Layer_all)
	str := cast.ToString(val)
	out := fmt.Sprintf("%s(%s)", title, str)
	return out
}

func (f *Field) WithMiddlewares(middlewareFns ApplyFns, fs ...*Field) *Field {
	middlewareFns.Apply(f, fs...)
	return f
}

func (f *Field) WithMiddleware(middlewareFn ApplyFn, fs ...*Field) *Field {
	middlewareFn.Apply(f, fs...)
	return f
}

func TrimNilField(field *Field, fn func(field *Field)) {
	if field == nil {
		return
	}
	fn(field)
}

// SetSceneIfEmpty 设置 Scene 场景，已第一次设置为准，建议在具体使用时设置，增加 insert,update,select 场景，方便针对场景设置，如enums, 下拉选择有全选，新增、修改没有
func (f *Field) SetSceneIfEmpty(scene Scene) *Field {
	if f.scene == "" {
		f.scene = scene
	}
	return f
}
func (f *Field) SetScene(scene Scene) *Field {
	f.scene = scene
	return f
}

// Scene  获取场景
func (f *Field) SceneFn(sceneFn SceneFn) *Field {
	f.sceneFns.Append(sceneFn)
	return f
}
func (f *Field) SceneFnRmove(name string) *Field {
	if len(f.sceneFns) > 0 {
		f.sceneFns.Remove(name)
	}
	return f
}
func (f *Field) Apply(applyFns ...ApplyFn) *Field {
	ApplyFns(applyFns).Apply(f)
	return f
}

// SetDelayApply 延迟执行中间件,在 xxx.ToSQL()中调用，在执行后才执行中间件(如在设置f.SetSelectColumns 时需要获取 f.Table().Columns 信息时，就需要延迟执行中间件)
func (f *Field) SetDelayApply(applyFns ...ApplyFn) *Field {
	f.delayApplies.Append(applyFns...)
	return f
}

func (f *Field) SceneInit(middlewareFns ...ApplyFn) *Field {
	f.Scene(NewScenes(SCENE_SQL_INIT), middlewareFns...)
	return f
}

func NewScenes(scenes ...Scene) Scenes {
	return scenes
}

func (f *Field) Scene(scenes Scenes, middlewareFns ...ApplyFn) *Field { // 批量设置场景，如除了查询场景，其它全部屏蔽，即可传入屏蔽函数，选择多个场景
	sceneFns := make([]SceneFn, 0)
	for _, scene := range scenes {
		for _, middlewareFn := range middlewareFns {
			sceneFns = append(sceneFns, SceneFn{
				Scene: scene,
				Fn:    middlewareFn,
			})
		}
	}
	f.sceneFns.Append(sceneFns...)
	return f
}

func (f *Field) SceneFinal(middlewareFns ...ApplyFn) *Field {
	sceneFns := make([]SceneFn, 0)
	for _, fn := range middlewareFns {
		sceneFns = append(sceneFns, SceneFn{
			Scene: SCENE_SQL_FINAL,
			Fn:    fn,
		})
	}
	f.sceneFns.Append(sceneFns...)
	return f
}
func (f *Field) SceneInsert(middlewareFn ApplyFn) *Field {
	f.Scene(NewScenes(SCENE_SQL_INSERT), middlewareFn)
	return f
}
func (f *Field) SceneSave(middlewareFn ApplyFn) *Field {
	f.Scene(NewScenes(SCENE_SQL_INSERT, SCENE_SQL_UPDATE), middlewareFn)
	return f
}
func (f *Field) SceneUpdate(middlewareFn ApplyFn) *Field {
	f.Scene(NewScenes(SCENE_SQL_UPDATE), middlewareFn)
	return f
}

func (f *Field) SceneSelect(middlewareFn ApplyFn) *Field {
	f.Scene(NewScenes(SCENE_SQL_SELECT), middlewareFn)
	return f
}

// Deprecated: 废弃，设置 SceneSelect 即可，目前保留函数签名，方便迭代升级，后续会删除此函数签名
func (f *Field) SceneExists(middlewareFn ApplyFn) *Field {
	// f.Scene(NewScenes(SCENE_SQL_EXISTS), middlewareFn)
	return f
}

// SetValue  设置第一个valueFn
func (f *Field) SetValue(value any) *Field {
	f.ValueFns.ResetSetValueFn(func(_ any, f *Field, fs ...*Field) (any, error) {
		return value, nil
	})
	return f
}

type FieldFn[T FieldTypeI] func(value T) *Field
type FieldFn2 func() *Field

func IsGenericByFieldFn(rt reflect.Type) bool {
	if rt.Kind() != reflect.Func {
		return false
	}
	if rt.NumIn() != 1 {
		return false
	}
	if rt.NumOut() != 1 {
		return false
	}
	returnT := rt.Out(0)
	canConvert := returnT.ConvertibleTo(reflect.TypeOf((*Field)(nil)))
	return canConvert
}

// GetName 获取字段名称, 是NewXXXField("").Name 的便捷函数
func (fn FieldFn[T]) GetName() string {
	valueRf := new(T)
	f := fn(*valueRf)
	name := f.Name
	return name
}

func GetFieldName[T FieldTypeI](fn FieldFn[T]) (fieldName string) {
	return fn.GetName()
}

func GetField[T FieldTypeI](fn FieldFn[T]) *Field {
	valueRf := new(T)
	f := fn(*valueRf)
	return f
}

func (fn FieldFn[T]) Apply(value T) *Field {
	return fn(value)
}

type FieldTypeIntI interface {
	~int | ~*int | ~int32 | ~int64 | ~uint | ~uint8 | ~uint32 | ~uint64 | ~*int64 | ~*uint64 | ~float64 | ~[]int | ~[]int64 | ~[]uint | ~[]uint8 | ~[]uint64 | ~[]float64 |
		~[]*int | ~[]*int64 | ~[]*uint | ~[]*uint8 | ~[]*uint64 | ~[]*float64
}

type FieldTypeStringI interface {
	~string | ~[]string | ~*string | ~[]*string
}
type FieldTypeBoolI interface {
	~bool | ~*bool | ~[]bool | ~[]*bool | ~*[]bool
}
type FieldTypeI interface {
	FieldTypeIntI |
		FieldTypeStringI |
		FieldTypeBoolI |
		ValueFn | ValueFnFn | func(inputValue any, f *Field, fs ...*Field) (any, error)
}

// NewField 生成列，使用最简单版本,只需要提供获取值的函数，其它都使用默认配置，同时支持修改（字段名、标题等这些会在不同的层级设置）
func NewField[T FieldTypeI](value T, middlewareFns ...ApplyFn) (field *Field) {
	field = &Field{}
	var valueFn ValueFn
	switch v := any(value).(type) {
	case func(inputValue any, f *Field, fs ...*Field) (any, error):
		valueFn = ValueFn{
			Fn:    v,
			Layer: Value_Layer_SetValue,
		}
	case ValueFnFn:
		valueFn = ValueFn{
			Fn:    v,
			Layer: Value_Layer_SetValue,
		}
	case ValueFn:
		valueFn = v
		if valueFn.Layer.IsEmpty() { // 默认设置
			valueFn.Layer = Value_Layer_SetValue
		}
	default:
		valueFn = ValueFn{
			Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
				return v, nil
			},
			Layer: Value_Layer_SetValue,
		}
	}

	field.ValueFns.Append(valueFn)
	ApplyFns(middlewareFns).Apply(field)
	return field
}

func NewIntField[T FieldTypeIntI](value T, name string, title string, maximum uint) (f *Field) {
	f = NewField(value).SetName(name).SetTitle(title).MergeSchema(Schema{
		Type: Schema_Type_int,
	})
	if maximum > 0 {
		f.MergeSchema(Schema{Maximum: maximum})
	}
	return f
}

func NewStringField[T FieldTypeStringI](value T, name string, title string, maxLength int) (f *Field) {
	f = NewField(value).SetName(name).SetTitle(title).MergeSchema(Schema{
		Type: Schema_Type_string,
	})
	if maxLength > 0 {
		f.MergeSchema(Schema{MaxLength: maxLength})
	}
	return f
}

var ErrValueNil = errors.New("error value nil")

func IsErrorValueNil(err error) bool {
	return errors.Is(err, ErrValueNil)
}

// ValueFnArgEmptyStr2NilExceptFields 将空字符串值转换为nil值时排除的字段,常见的有 deleted_at 字段,空置代表正常
//var ValueFnArgEmptyStr2NilExceptFields = Fields{}

var GlobalFnValueFns = func(f Field, fs ...*Field) ValueFns {
	return ValueFns{
		//GlobalValueFnEmptyStr2Nil(f, ValueFnArgEmptyStr2NilExceptFields...), // 将空置转换为nil,代替对数据判断 if v==""{//ignore}  这个函数在全局修改了函数值，出现问题，比较难跟踪，改到每个组件自己处理
		ValueFnDBSchemaFormatType(f), // 在转换为SQL前,将所有数据类型按照DB类型转换,主要是格式化int和string,提升SQL性能，将数据格式改成DB格式，不影响当期调用链，可以作为全局配置
		ValueFnTrimBlankSpace,
		MergeDefaultValue,
		//todo 统一实现数据库字段前缀处理
		//todo 统一实现代码字段驼峰形转数据库字段蛇形
		//todo 统一实现数据库字段替换,方便数据库字段更名
		//todo 统一实现数据库字段屏蔽,方便废弃数据库字段
		//todo 虽然单次只有一个字段信息,但是所有SQL语句的字段都一定经过该节点,这就能收集到全量信息,进一步拓展其用途如(发布事件,其它订阅):
		//todo 1. 统一收集数据库字段名形成数据字典
		//todo 2. 统一收集api字段生成文档
		//todo ...
	}
}

// Deprecated: 废弃，就像Field.dbName 一样，交由上层映射，不做内部转换（调用方持有Field.Name,不是Field 持有 docName）
// func (f *Field) SetDocName(docName string) *Field {
// 	f.docName = docName
// 	return f
// }

// Deprecated: 废弃，就像Field.dbName 一样，交由上层映射，不做内部转换（调用方持有Field.Name,不是Field 持有 docName）
// func (f Field) GetDocName() string {
// 	if f.docName == "" {
// 		f.docName = f.Name
// 	}
// 	return f.docName
// }

// InitBeforeCalValue 实际执行，计算值前的初始化
func (f *Field) InitBeforeCalValue(fs ...*Field) *Field {
	if f.sceneFns != nil {

		initFns := f.sceneFns.GetByScene(SCENE_SQL_INIT) //init 场景每次都运行
		for _, sceneFn := range initFns {
			sceneFn.Fn.Apply(f, fs...)
		}

		sceneFns := f.sceneFns.GetByScene(f.scene)
		for _, sceneFn := range sceneFns {
			sceneFn.Fn.Apply(f, fs...)
		}

		finalFns := f.sceneFns.GetByScene(SCENE_SQL_FINAL) // final 场景每次都执行
		for _, sceneFn := range finalFns {
			sceneFn.Fn.Apply(f, fs...)
		}

	}
	if f.Schema == nil {
		f.Schema = &Schema{
			Enums: make(Enums, 0),
		}
	}
	f.Schema.Enums.Sort()
	return f
}

func (f Field) InjectValueFn(fs ...*Field) Field {
	f.ValueFns.Append(ValueFn{
		Fn: func(in any, f *Field, fs ...*Field) (any, error) { //插入数据验证
			err := f.Validate(in)
			if err != nil {
				return in, err
			}
			return in, nil
		},
		Layer: Value_Layer_ApiValidate,
	})
	f.ValueFns.Append(GlobalFnValueFns(f)...) // 在最后生成SQL数据时追加格式化数据
	return f
}

func (f Field) GetValue(layers []Layer, fs ...*Field) (value any, err error) {
	f = f.InjectValueFn(fs...)
	return f.getValue(layers, fs...)
}

func (f Field) getValue(layers []Layer, fs ...*Field) (value any, err error) {
	if f.ValueFns == nil { // 防止空指针
		return nil, nil
	}
	valueFns := f.ValueFns.GetByLayer(layers...)
	value, err = valueFns.Value(nil, &f, fs...)
	if err != nil {
		return value, err
	}
	if IsNil(value) {
		err = ErrValueNil //相比返回 nil,nil; 此处抛出错误，其它地方更容易感知中断处理，如需要继续执行，执行忽略这个类型Error 即可
		return nil, err
	}
	return value, nil
}

// WhereData 获取Where 值
func (f1 Field) WhereData(layers []Layer, fs ...*Field) (value any, err error) {
	f := *f1.Copy()
	f.InitBeforeCalValue(fs...)
	if len(f.WhereFns) == 0 {
		return nil, nil
	}
	// 已经在layers 中过滤了OnlyForData 层，注释观察，后续可删除
	// if len(f.ValueFns) > 0 {
	// 	f.ValueFns = _ExcludeOnlyForDataValueFn(f.ValueFns)
	// }
	value, err = f.GetValue(layers, fs...)
	if IsErrorValueNil(err) {
		err = nil // 这里不直接返回，仍然遍历 执行whereFns，方便理解流程（直接返回后，期望的whereFn没有执行，不知道流程在哪里中断了，也没有错误抛出，非常困惑，所以不能直接返回）
	}
	if err != nil {
		return value, err
	}
	for _, fn := range f.WhereFns {
		if fn.IsNil() {
			continue
		}
		value, err = fn.Fn(value, &f, fs...) // value 为nil 继续循环，主要考虑调试方便，若中途中断，可能导致调试困难(代码未按照预期运行，不知道哪里中断了)，另外一般调试时，都没有写参数值，方便能快速查看效果
		if err != nil {
			return value, err
		}
	}
	if IsNil(value) {
		return nil, nil
	}

	return value, nil
}

func FilterNil(in any, valueFn ValueFn) (any, error) {
	if IsNil(in) {
		return nil, nil
	}
	return valueFn.Fn(in, nil)
}

// IsEqual 判断名称值是否相等
func (f Field) IsEqual(otherF Field, fs ...*Field) bool {
	fv, err := f.GetValue(Layer_all, fs...)
	if err != nil || IsNil(fv) {
		return false
	}
	ov, err := otherF.GetValue(Layer_all, fs...)
	if err != nil || IsNil(ov) {
		return false
	}
	return strings.EqualFold(cast.ToString(fv), cast.ToString(ov)) && strings.EqualFold(f.Name, otherF.Name)
}

func (c Field) Validate(val any) (err error) {
	if c.Schema == nil {
		return nil
	}
	// nil 值也需要校验，当 c.Schema.Required 为 true 时，需要返回false（数组类型容易出现nil, c.Schema.Required =true）
	// if IsNil(val) {
	// 	return nil
	// }

	rv := reflect.Indirect(reflect.ValueOf(val))
	err = c.Schema.Validate(c.Name, rv)
	if err != nil {
		return err
	}

	return
}

func (c Field) FormatType(val any) (value any) {
	if IsNil(val) {
		return val
	}
	switch val.(type) {
	case exp.LiteralExpression:
		return val
	}

	value = val
	if c.Schema == nil {
		return value
	}

	valValue := reflect.Indirect(reflect.ValueOf(val))
	valType := valValue.Type()
	switch valType.Kind() {
	case reflect.Slice, reflect.Array:
		var dstTyp any
		switch c.Schema.Type {
		case Schema_Type_int:
			dstTyp = 0
		default:
			dstTyp = reflect.Zero(valType.Elem()).Interface()
		}

		formattedSlice := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(dstTyp)), valValue.Len(), valValue.Len())
		for i := 0; i < valValue.Len(); i++ {
			formattedSlice.Index(i).Set(reflect.ValueOf(c.formatSingleType(valValue.Index(i).Interface())))
		}
		return formattedSlice.Interface()
	}
	return c.formatSingleType(val)
}

func (c Field) formatSingleType(val any) any {
	var value = val
	switch c.Schema.Type {
	case Schema_Type_string:
		value = cast.ToString(value)
	// case Schema_Type_json:
	// 	b, _ := json.Marshal(value)
	// 	value = string(b)
	case Schema_Type_int:
		value = cast.ToInt(value)
	}
	return value
}

func (f1 Field) Data(layers []Layer, fs ...*Field) (data any, err error) {
	f := *f1.Copy() // 复制一份,不影响其它场景
	f.InitBeforeCalValue(fs...)

	val, err := f.GetValue(layers, fs...)
	if IsErrorValueNil(err) {
		return nil, nil // 忽略空值错误
	}
	if err != nil {
		return nil, err
	}
	if f.scene.Is(SCENE_SQL_UPDATE) && f.Schema != nil && f.Schema.ShieldUpdate { // 当前为更新场景，并且设置屏蔽更新，则返回nil
		return nil, nil
	}
	// 此处多次实践，发现基本的转义，在sql中已经完成，基本不用再进行转义处理了，除非特殊场景需要手动转义，例如：json序列化内嵌套序列化，情况比较有限，不作为默认处理
	// if valStr, ok := val.(string); ok {
	// 	val = Dialect.EscapeString(valStr)
	// }
	data = map[string]any{
		f.DBColumnName().BaseName(): val, // Data 函数只用于insert,update 写入数据部分,只用基础名称即可(insert 带有全名时,gorm报错)
	}
	return data, nil
}

func (f Field) Where(fs ...*Field) (expressions Expressions, err error) {
	val, err := f.WhereData(Layer_where, fs...)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	dbName := f.DBColumnName().FullName()
	if ex, ok := TryParseExpressions(dbName, val); ok {
		return ex, nil
	}
	return ConcatExpression(goqu.Ex{dbName: val}), nil
}

func (f Field) Order(fs ...*Field) (orderedExpressions []exp.OrderedExpression) {
	orderedExpressions = make([]exp.OrderedExpression, 0)
	if f._OrderFnWithSort.Fn != nil {
		exs := f._OrderFnWithSort.Fn(&f, fs...)
		realExs := make([]exp.OrderedExpression, 0)
		if len(exs) > 0 {
			for _, v := range exs {
				switch sortE := v.SortExpression().(type) {
				case exp.IdentifierExpression:
					col := cast.ToString(sortE.GetCol())
					if col != "" { // 过滤排序字段为空的错误设置情况(考虑返回错误，会变复杂，panic处理不友好，考虑到即便panic这个错误,上层也只能这样处理,所以暂时过滤掉，后续再优化处理)
						realExs = append(realExs, v)
					}
				default:
					realExs = append(realExs, v)
				}

			}
		}
		orderedExpressions = append(orderedExpressions, realExs...)
	}
	return orderedExpressions
}

func ValueFnSetValue(valueFnFn ValueFnFn) ValueFn {
	return ValueFn{
		Fn:          valueFnFn,
		Layer:       Value_Layer_SetValue,
		Description: "api 设置数据时机执行", // 描述
	}
}
func ValueFnSetFormat(valueFnFn ValueFnFn) ValueFn {
	return ValueFn{
		Fn:          valueFnFn,
		Layer:       Value_Layer_SetFormat,
		Description: "设置数据后,验证前执行", // 描述
	}
}
func ValueFnApiFormat(valueFnFn ValueFnFn) ValueFn {
	return ValueFn{
		Fn:          valueFnFn,
		Layer:       Value_Layer_ApiFormat,
		Description: "api 格式化数据时机执行", // 描述
	}
}

const (
	Tag_validate_At_least_one = "tag_validate_At_least_one"
)

func ValueFnApiValidateAtLeastOne(TagAtLeastOne string) ValueFn {
	return ValueFn{
		Name: Tag_validate_At_least_one,
		Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
			subFields := Fields(fs).GetByTags(TagAtLeastOne)
			data, err := subFields.Data(Value_Layer_SetValue, Value_Layer_SetFormat)
			if err != nil {
				return nil, err
			}
			if IsNil(data) {
				nameArr := make([]string, 0)
				_ = subFields.Each(func(f *Field) error {
					nameArr = append(nameArr, f.Name)
					return nil
				})
				err = errors.Errorf("at least one of[%s] required", strings.Join(nameArr, ","))
				return nil, err
			}

			return inputValue, nil
		},
		Layer:       Value_Layer_ApiValidate,
		Description: "api 验证数据时机执行", // 描述
	}
}

func ValueFnDBFormat(fn func(in any, f *Field, fs ...*Field) (any, error)) ValueFn {
	return ValueFn{
		Fn:          fn,
		Layer:       Value_Layer_DBFormat,
		Description: "db 格式化数据时机执行", // 描述
	}
}

func WrapWithSkipNil(fn func(in any, f *Field, fs ...*Field) (any, error)) ValueFnFn {
	return func(in any, f *Field, fs ...*Field) (any, error) {
		if IsNil(in) {
			return nil, nil
		}
		return fn(in, f, fs...)
	}

}

func ValueFnApiValidate(valueFnFn ValueFnFn) ValueFn {
	return ValueFn{
		Fn:          valueFnFn,
		Layer:       Value_Layer_ApiValidate,
		Description: "api 验证数据时机执行", // 描述
	}
}

func ValueFnOnlyForData(valueFnFn ValueFnFn) ValueFn {
	return ValueFn{
		Fn:          valueFnFn,
		Layer:       Value_Layer_OnlyForData,
		Description: "当计算where条件时不使用,仅用于insert,update 的 set 部分", // 描述
	}
}

// func OlyForWhereValueFn(valueFnFn ValueFnFn) ValueFn {
// 	return ValueFn{
// 		Fn:          valueFnFn,
// 		Layer:       Value_Layer_OnlyForWhere,
// 		Description: "不用于insert,update 写数据部分，仅用于wehre中", // 描述
// 	}
// }

type FieldFilterFn func(f Field) bool

// FieldFilterExclude 从fields 集合中筛选出和subFileds差集
func FieldFilterExclude(subFileds ...*Field) FieldFilterFn {
	return func(f Field) bool {
		for _, subField := range subFileds {
			ok := strings.EqualFold(subField.Name, f.Name)
			if ok {
				return false
			}
		}
		return true
	}
}

// FieldFilterInclude 从fields 集合中筛选出和subFileds交集
func FieldFilterInclude(subFileds ...*Field) FieldFilterFn {
	return func(f Field) bool {
		for _, subField := range subFileds {
			ok := strings.EqualFold(subField.Name, f.Name)
			if ok {
				return true
			}
		}
		return false
	}
}

type FieldsI interface {
	Fields() Fields
}

type Fields []*Field

func NewFields(fs ...*Field) Fields {
	return fs
}

func (fs Fields) Fields() Fields {
	return fs
}
func (fs Fields) IsEmpty() bool {
	return len(fs) == 0
}

func (fs Fields) FirstMust() *Field {
	if len(fs) == 0 {
		err := errors.Errorf("Fields is empty")
		panic(err)
	}
	return fs[0]
}

//GetBySampleField 根据样板(未完全配置的初始化字段)获取对应的配置完备的字段，如果没有则返回样板本身，常用于从fields集合中筛选字段

func (fs Fields) GetBySampleField(field *Field) (f *Field) {
	f, ok := fs.GetByName(field.Name)
	if !ok {
		return field
	}
	return f
}

func (fs Fields) Fielter(fn FieldFilterFn) (fields Fields) {
	fields = make(Fields, 0)
	for _, f := range fs {
		if fn(*f) {
			fields = append(fields, f)
		}
	}
	return fields
}
func (fs Fields) Each(fn func(f *Field) error) error {
	for _, f := range fs {
		err := fn(f)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fs Fields) ApplyCunstomFn(customFns ...CustomFieldsFn) (newFs Fields) {
	for _, customFn := range customFns {
		if customFns != nil {
			fs = customFn(fs)
		}
	}
	return fs
}

func (fs Fields) Copy() (fields Fields) {
	fields = make(Fields, 0)
	for _, f := range fs {
		fields = append(fields, f.Copy())
	}
	return fields
}

func (fs Fields) Builder(ctx context.Context, scene Scene, tableConfig TableConfig, customFns CustomFieldsFns) (fields Fields) {
	fields = fs.Copy()                    // 使用复制版本，后续调整部分初始化函数到这里
	fields = fields.SetTable(tableConfig) // 将表名设置到字段中,方便在ValueFn 中使用table变量
	fields = fields.ApplyCunstomFn(customFns...)

	fields = fields.SetTable(tableConfig) // 确保新增字段有table信息
	fields = tableConfig.MergeTableLevelFields(ctx, fields...)

	fields = fields.SetTable(tableConfig) // 确保新增字段有table信息
	fields = fields.ApplyDelay()          // 复制完成后再执行延迟中间件，因为Builder可能运行多次，不能污染最初的fs 变量

	fields = fields.SetTable(tableConfig)  // 最后确保所有字段有table信息
	fields = fields.SetSceneIfEmpty(scene) // 确保所有字段有场景信息
	return fields
}

// Validate 方便前期校验
func (fs Fields) Validate() (err error) {
	for _, f := range fs {
		f.InitBeforeCalValue(fs...)
		_, err = f.GetValue(Layer_Validate, fs...)
		if err != nil {
			return err
		}
	}
	return err
}

func (fs Fields) SetSceneIfEmpty(scene Scene) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SetSceneIfEmpty(scene)
	}
	return fs
}

// SetTable 设置表,不存在直接设置,存在则合并表配置信息
func (fs Fields) SetTable(table TableConfig) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SetTable(table)
	}
	return fs
}

// MergeMatchedTable 匹配表，更新表字段表配置信息，用于多表查询时，字段归属表不清晰的情况。例如： 多表join查询
func (fs Fields) MergeMatchedTable(tables ...TableConfig) Fields {
	ts := TableConfigs(tables)
	for i := 0; i < len(fs); i++ {
		t, exists := ts.GetByName(fs[i].table.Name)
		if exists {
			fs[i].SetTable(*t)
		}
	}
	return fs
}

func (fs Fields) Tables() []string {
	m := map[string]struct{}{}
	for i := 0; i < len(fs); i++ {
		m[fs[i].table.Name] = struct{}{}
	}
	tables := make([]string, 0)
	for k := range m {
		if k != "" {
			tables = append(tables, k)
		}
	}
	return tables
}

func (fs Fields) SetScene(scene Scene) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SetScene(scene)
	}
	return fs
}
func (fs Fields) SceneInsert(fn ApplyFn) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SceneInsert(fn)
	}
	return fs
}

func (fs Fields) MiddlewareSceneUpdate(fn ApplyFn) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SceneUpdate(fn)
	}
	return fs
}

func (fs Fields) SceneSelect(fn ApplyFn) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SceneSelect(fn)
	}
	return fs
}

func (fs Fields) Select() (columns []any) {
	columns = make([]any, 0)
	for _, f := range fs {
		columns = append(columns, f.Select()...)
	}
	return columns
}

// Intersection 交集，多用于模型封装时求表字段交集
func (fs Fields) Intersection(assistant Fields) (intersection Fields) {
	primaryTable := memorytable.NewTable(fs...)
	assistantTable := memorytable.NewTable(assistant...)
	out, _ := primaryTable.Intersection(assistantTable, func(row *Field) string {
		return row.Name
	})
	intersection, _ = out.ToSliceWithEmpty()
	return intersection
}
func (fs Fields) IntersectionUnionRequired(assistant Fields) (IntersectionUnionRequired Fields) {
	requiredFields, nonRequiredFields := fs.SplitRequired()
	intersection := nonRequiredFields.Intersection(assistant)
	IntersectionUnionRequired = requiredFields.Add(intersection...)
	return IntersectionUnionRequired
}

// SplitRequired 区分必填和非必填字段，模型封装时分离出非必要字段和表字段求交集,必填字段不能缺失
func (fs Fields) SplitRequired() (requiredFields, nonRequiredFields Fields) {
	for _, f := range fs {
		if f.IsRequired() {
			requiredFields = append(requiredFields, f)
		} else {
			nonRequiredFields = append(nonRequiredFields, f)
		}
	}
	return requiredFields, nonRequiredFields
}

func (fs Fields) Pagination() (index uint, size uint) {
	if pageIndex, ok := fs.GetByTag(Field_tag_pageIndex); ok {
		val, _ := pageIndex.GetValue(Layer_get_value_before_db, fs...)
		index = cast.ToUint(val)

	}
	if pageSize, ok := fs.GetByTag(Field_tag_pageSize); ok {
		val, _ := pageSize.GetValue(Layer_get_value_before_db, fs...)
		size = cast.ToUint(val)
	}
	index, size = max(index, 0), max(size, 0)
	return index, size
}
func (fs Fields) DeletedAt() (f *Field, err error) {
	f, ok := fs.GetByFieldName(Field_name_deletedAt)
	if !ok {
		err = errors.Errorf("not found deleted column by fieldName:%s", Field_name_deletedAt)
		return nil, err
	}
	return f, nil
}

func (fs Fields) Limit() (limit uint) {
	_, pageSize := fs.Pagination()
	limit = pageSize
	return limit
}

func (fs Fields) Contains(field Field) (exists bool) {
	for _, f := range fs {
		if strings.EqualFold(f.Name, field.Name) { // 暂时值判断名称,后续根据需求,再增加类型
			return true
		}
	}
	return false
}

func (fs *Fields) Apply(applyFns ...ApplyFn) *Fields {
	for i := 0; i < len(*fs); i++ {
		(*fs)[i] = (*fs)[i].Apply(applyFns...)
	}
	return fs
}

// ApplyDelay 执行延迟中间件，不影响已有的Feilds
func (fs Fields) ApplyDelay() Fields {
	fields := make(Fields, 0)
	for _, f := range fs {
		fields = append(fields, f.Apply(f.delayApplies...))
	}
	return fields
}

//MakeDBColumnWithAlias 生成查询字段别名，用于多表联合查询时，字段同名问题处理,模块预封装场景

func (fs Fields) MakeDBColumnWithAlias(tableColumns ColumnConfigs) (selectColumnWithAlias []any) {
	selectColumnWithAlias = make([]any, 0)
	for _, f := range fs {
		selectColumnWithAlias = append(selectColumnWithAlias, f.MakeDBColumnWithAlias(tableColumns))
	}
	return selectColumnWithAlias
}

// MakeAsOneDBColumnWithAlias 将数据表多个字段值合并成一个，并取别名（主要用于多字段生成唯一标识场景,注意tableColumns 的顺序就是拼接顺序，调用者可以基于表索引排序，或者自然排序好）
func (fs Fields) MakeAsOneDBColumnWithAlias(alias string, tableColumns ColumnConfigs) (selectColumnWithAlias any) {
	if len(fs) == 1 {
		return fs[0].MakeDBColumnWithAlias(tableColumns)
	}
	arr := make([]string, 0)
	//
	//slices.SortStableFunc(fs, func(a *Field, b *Field) int { return a.ddlSequence - b.ddlSequence })
	lastIndex := len(fs) - 1
	//CONCAT(`key`,"-",`value`) as `key_value`
	for i, f := range fs {
		col := tableColumns.GetByFieldNameMust(f.Name)
		arr = append(arr, fmt.Sprintf("`%s`", col.DbName))
		if i != lastIndex {
			arr = append(arr, `"-"`)
		}
	}
	concatSql := fmt.Sprintf("CONCAT(%s)", strings.Join(arr, ","))
	aliasSQL := goqu.L(concatSql).As(alias)
	return aliasSQL
}

// Add 新增列，Append 被占用，使用add
// 2023-07-25 14:05:40   add 方法名称容易理解成在原有fs基础上最佳，经常写成 fs.Add(fs2...), 忘记要写成fs = fs.Add(fs2...), 建议改成引用func (fs *Fields) 方式
//2025-07-30 10:20:48 Add 使用Fields 方便链式调用，换成 *Fields 不能和其它方法链式调用，为了避免上述问题，新增AddRef 方法

func (fs Fields) Add(moreFs ...*Field) Fields {
	fs = append(fs, moreFs...)
	return fs
}

func (fs *Fields) AddRef(moreFs ...*Field) Fields {
	*fs = append(*fs, moreFs...)
	return *fs
}

// Deprecated: 废弃，使用 Add 方法代替 Fields 容许重复
func (fs *Fields) Append(moreFields ...*Field) *Fields {
	if *fs == nil {
		*fs = make(Fields, 0)
	}
	for _, f := range moreFields {
		exists := false
		for i := range *fs {
			if (*fs)[i].Name == f.Name {
				(*fs)[i].Combine(f) // 合并配置
				break
			}
		}
		if !exists {
			*fs = append(*fs, f)
		}
	}
	return fs
}

func (fs *Fields) Replace(fields ...*Field) *Fields {
	if *fs == nil {
		*fs = make(Fields, 0)
	}
	for _, f := range fields {
		exists := false
		for i := 0; i < len(*fs); i++ {
			if (*fs)[i].Name == f.Name {
				(*fs)[i] = f
				exists = true
				break

			}
		}
		if !exists {
			fs.Append(f)
		}
	}
	return fs
}

func (fs Fields) Remove(fields ...*Field) *Fields {
	subFs := make(Fields, 0)
	removeFields := Fields(fields)
	for i := 0; i < len(fs); i++ {
		if !removeFields.Contains(*fs[i]) {
			subFs = append(subFs, fs[i])
		}
	}
	return &subFs
}

func (fs Fields) WithMiddlewares(middlewares ...ApplyFn) Fields {
	for _, f := range fs {
		ApplyFns(middlewares).Apply(f, fs...)
	}
	return fs
}

func (fs Fields) Middleware(fns ...ApplyFn) Fields {
	for i := 0; i < len(fs); i++ {
		for _, fn := range fns {
			fn(fs[i], fs...)
		}
	}

	return fs
}

func (fs Fields) AppendWhereValueFn(whereValueFns ...ValueFn) Fields {

	for i := 0; i < len(fs); i++ {
		fs[i].WhereFns.Append(whereValueFns...)
	}

	return fs
}

func (fs Fields) Names() (names []string) {
	names = make([]string, 0)
	for _, f := range fs {
		names = append(names, f.Name)
	}
	return names
}

// DocNameWrapArrFn 将文档列名称前增加数组符号
func DocNameWrapArrFn(name string) string {
	return fmt.Sprintf("[].%s", name)
}

func (fs Fields) Json() string {
	b, _ := json.Marshal(fs)
	return string(b)
}
func (fs Fields) String() string {
	m := make(map[string]any)
	for _, f := range fs {
		val, _ := f.GetValue(Layer_all, fs...)
		m[f.DBColumnName().FullName()] = val
	}
	b, _ := json.Marshal(m)
	return string(b)
}
func (fs Fields) GetByTag(tag string) (f *Field, ok bool) {
	for i := 0; i < len(fs); i++ {
		if fs[i].HastTag(tag) {
			return fs[i], true
		}
	}
	return nil, false
}

func (fs Fields) GetByTags(tags ...string) (subFs Fields) {
	subFs = make(Fields, 0)
	for i := 0; i < len(fs); i++ {
		for _, tag := range tags {
			if fs[i].HastTag(tag) {
				subFs = append(subFs, fs[i])
			}
		}
	}
	return subFs
}

func (fs Fields) GetByFieldName(fieldName string) (*Field, bool) {
	for i := 0; i < len(fs); i++ {
		f := fs[i]
		if strings.EqualFold(fieldName, f.fieldName) {
			return f, true
		}
	}
	return nil, false
}

// GetByName 通过名称获取field, 也可用户判断指定name是否存在
func (fs Fields) GetByName(name string) (*Field, bool) {
	for i := range fs {
		if strings.EqualFold(name, fs[i].Name) {
			return fs[i], true
		}
	}
	return nil, false
}

func (fs Fields) GetByNameMust(name string) *Field {
	for i := range fs {
		if strings.EqualFold(name, fs[i].Name) {
			return fs[i]
		}
	}
	err := errors.Errorf("not found Field by name:%s", name)
	panic(err)
}

func (fs Fields) DBNames() (dbNames []string, err error) {
	dbNames = make([]string, 0)
	for _, f := range fs {
		dbNames = append(dbNames, f.DBColumnName().FullName())
	}
	return dbNames, nil
}

func (fs Fields) Where() (expressions Expressions, err error) {
	expressions = make(Expressions, 0)
	for _, field := range fs {
		subExprs, err := field.Where(fs...)
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, subExprs...)
	}
	return expressions, nil
}

func (fs Fields) sortByOrderField() Fields {
	cp := fs.Copy()
	slices.SortFunc(cp, func(a, b *Field) int {
		return a._OrderFnWithSort.Sort - b._OrderFnWithSort.Sort
	})
	return cp
}

func (fs Fields) Order() (orderedExpressions []exp.OrderedExpression) {
	orderedExpressions = make([]exp.OrderedExpression, 0)
	fs = fs.sortByOrderField()
	for _, field := range fs {
		subExprs := field.Order(fs...)
		orderedExpressions = append(orderedExpressions, subExprs...)
	}
	return orderedExpressions
}

func (fs Fields) Data(layers ...Layer) (data any, err error) {
	if len(layers) == 0 {
		layers = append(layers, Value_Layer_SetValue)
	}
	dataMap := make(map[string]any, 0)
	for _, f := range fs {
		data, err := f.Data(layers, fs...)
		if err != nil {
			return nil, err
		}
		if IsNil(data) {
			continue
		}
		if m, ok := data.(map[string]any); ok { // 值接收map[string]any 格式
			for k, v := range m {
				dataMap[k] = v
			}
		}
	}
	isNil := true
	for range dataMap {
		isNil = false
		break
	}
	if isNil {
		return nil, nil
	}
	return dataMap, nil
}

func IsNil(val any) bool {
	if val == nil {
		return true
	}
	valueOf := reflect.ValueOf(val)
	k := valueOf.Kind()
	switch k {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return valueOf.IsNil()
	default:
		return val == nil
	}
}

type Neq struct { // type Neq any  在 iLike, ok := value.(Ilike); ok恒等于true，所以改成结构体
	Value any
}

func TryNeq(field string, value any) (expressions Expressions, ok bool) {
	if val, ok := value.(Neq); ok {
		identifier := goqu.I(field)
		return ConcatExpression(identifier.Neq(val)), true
	}
	return nil, false
}

// Ilike 不区分大小写like语句
type Ilike [3]any

func (iLike Ilike) String() (ilikeVal string) {
	strArr := make([]string, 0)
	for _, arg := range iLike {
		str := cast.ToString(arg)
		if str != "" {
			strArr = append(strArr, str)
		}
	}
	ilikeVal = strings.Join(strArr, "")
	return ilikeVal
}

func TryIlike(field string, value any) (expressions Expressions, ok bool) {
	if iLike, ok := value.(Ilike); ok {
		identifier := goqu.I(field)
		val := iLike.String()
		return ConcatExpression(identifier.ILike(val)), true
	}
	return nil, false
}

// GlobalFnFormatFieldName 全局函数钩子,统一修改字段列名称,比如统一增加列前缀F
var GlobalFnFormatFieldName = func(fieldName string) string {
	return fieldName
}

// GlobalFnFormatTableName 全局函数钩子,统一修改表名称,比如统一增加表前缀t_
var GlobalFnFormatTableName = func(tableName string) string {
	return tableName
}

// Deprecated  NewBetweenWithoutEmpty 废弃，直接使用ValueFnBetweenWitEmptyNil(start,end)代替
func NewBetweenWithoutEmpty[T int | int64 | float64 | string](start T, end T) Between {
	start1, _ := ValueFnEmpty2Nil.Fn(start, nil)
	end1, _ := ValueFnEmpty2Nil.Fn(end, nil)
	return Between{start1, end1}
}

// Between 介于2者之间(包含上下边界，对于不包含边界情况，可以修改值范围或者直接用表达式),3个元素时为: col1<12<col2 格式,2个元素时为: 12<=col1<=14,1个元素看在数组中的位置

type Between [3]any

func (b Between) Empty2Nil() (newB Between) {
	newB = b
	for i := range newB {
		newB[i] = Empty2Nil(newB[i])
	}
	return newB
}

func TryConvert2Betwwen(field string, value any) (expressions Expressions, ok bool) {
	if between, ok := value.(Between); ok {
		between = between.Empty2Nil()
		identifier := goqu.I(field)
		min, val, max := between[0], between[1], between[2]
		if min == nil && max == nil && val == nil {
			return nil, true // 3个元素都为空，返回nil
		}

		if max != nil {
			expressions = ConcatExpression(goqu.L("?", val).Between(exp.NewRangeVal(goqu.I(cast.ToString(min)), goqu.I(cast.ToString(max)))))
			return expressions, true
		}
		max = val // 当作2个值处理
		if !IsNil(min) && !IsNil(max) {
			expressions = append(expressions, identifier.Between(exp.NewRangeVal(min, max)))
			return expressions, true
		}
		if !IsNil(min) {
			return ConcatExpression(identifier.Gte(min)), true
		}

		if !IsNil(max) {
			return ConcatExpression(identifier.Lte(max)), true
		}
	}
	return nil, false
}

// TryConvert2Expressions 业务where 条件判断，优先判断是否符可以转换为条件，可以直接应用
func TryConvert2Expressions(value any) (expressions Expressions, ok bool) {
	if ex, ok := value.(Expressions); ok {
		return ex, true
	}
	if ex, ok := value.(goqu.Expression); ok {
		return ConcatExpression(ex), true
	}
	return nil, false
}

// TryParseExpressions 尝试解析where条件
func TryParseExpressions(field string, value any) (expressions Expressions, ok bool) {
	if ex, ok := TryConvert2Expressions(value); ok {
		return ex, true
	}
	if ex, ok := TryConvert2Betwwen(field, value); ok {
		return ex, true
	}

	if ex, ok := TryIlike(field, value); ok {
		return ex, true
	}
	if ex, ok := TryNeq(field, value); ok {
		return ex, true
	}

	return nil, false
}

// Deprecated: 请使用Expression2StringWithDriver
func Expression2String(expressions ...goqu.Expression) string {
	sql, _, _ := Dialect.DialectWrapper().Select().Where(expressions...).ToSQL()
	return sql
}

func Expression2StringWithDriver(driver Driver, expressions ...goqu.Expression) string {
	sql, _, _ := driver.GoquDialect().Select().Where(expressions...).ToSQL()
	return sql
}

type FieldName2DBColumnNameFn func(fieldName string) (dbColumnName string)

func FieldName2DBColumnNameFnDirect() FieldName2DBColumnNameFn {
	return func(fieldName string) (dbColumnName string) {
		return fieldName
	}
}
func (tcFn FieldName2DBColumnNameFn) WithSnakeCase() FieldName2DBColumnNameFn {
	return func(fieldName string) (dbColumnName string) {
		return funcs.ToSnakeCase(tcFn(fieldName))
	}
}
func (tcFn FieldName2DBColumnNameFn) WithPrefix(prefix string) FieldName2DBColumnNameFn {
	return func(fieldName string) (dbColumnName string) {
		dbColumnName = tcFn(fieldName)
		return fmt.Sprintf("%s%s", prefix, dbColumnName)
	}
}

func (tcFn FieldName2DBColumnNameFn) WithSnakeCaseAndPrefix(prefix string) FieldName2DBColumnNameFn {
	return tcFn.WithSnakeCase().WithPrefix(prefix)
}

// Deprecated  请使用TableConfig 设置
// FieldName2DBColumnName 将接口字段转换为数据表字段列名称
var FieldName2DBColumnName FieldName2DBColumnNameFn = func(fieldName string) (dbColumnName string) {
	dbColumnName = funcs.ToSnakeCase(fieldName)
	dbColumnName = fmt.Sprintf("F%s", strings.TrimPrefix(dbColumnName, "F")) // 增加F前缀
	return dbColumnName
}

// StructToFields 将结构体结构的fields 转换成 数组类型 结构体格式的feilds 方便编程引用, 数组类型fields 方便当作数据批量处理,常用于生产文档、ddl等场景,支持对象属性、数组类型定制化
func StructToFields(stru any,
	structFieldCustomFn func(val reflect.Value, structField reflect.StructField, fs Fields) Fields,
	arrayFieldCustomFn func(fs Fields) Fields,
) Fields {
	fs := Fields{}
	if stru == nil {
		return fs
	}
	val := reflect.Indirect(reflect.ValueOf(stru))
	fs = structToFields(val, structFieldCustomFn, arrayFieldCustomFn)
	return fs
}

func structToFields(val reflect.Value,
	structFieldCustomFn func(val reflect.Value, structField reflect.StructField, fs Fields) Fields,
	arrayFieldCustomFn func(fs Fields) Fields,
) Fields {
	val = reflect.Indirect(val)
	fs := Fields{}
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if !val.IsValid() {
		return fs
	}
	typ := val.Type()

	switch typ.Kind() {
	// 整型、字符串 等基本类型不处理, 对于使用 FieldsI 接口的,充分应用了这点
	case reflect.Func:
		if val.IsNil() {
			break
		}
		funcVal := val.Call([]reflect.Value{reflect.Zero(val.Type().In(0))})[0]
		if funcVal.CanInterface() {
			if fld, ok := funcVal.Interface().(*Field); ok {
				fs = append(fs, fld)
			}
		}
	case reflect.Struct:
		isImplementedFieldI := typ.Implements(reflect.TypeOf((*FieldsI)(nil)).Elem())
		if isImplementedFieldI {
			hasFieldAttr := false // 判断属性是否包含 *Field类型，如果包含，则 不能调用 Fields() 方法(会循环调用)
			for i := 0; i < val.NumField(); i++ {
				attrType := val.Field(i).Type()
				if IsGenericByFieldFn(attrType) {
					hasFieldAttr = true
					break
				}
			}

			if !hasFieldAttr {
				if fieldsI, ok := val.Interface().(FieldsI); ok {
					fs = append(fs, fieldsI.Fields()...)
				}
			}
		}
		for i := 0; i < val.NumField(); i++ {
			field := val.Field(i)
			attr := typ.Field(i)
			subFields := structToFields(field, structFieldCustomFn, arrayFieldCustomFn)
			if structFieldCustomFn != nil {
				subFields = structFieldCustomFn(field, attr, subFields)
			}
			fs = append(fs, subFields...)

		}

	case reflect.Array, reflect.Slice:
		childTyp := typ.Elem()
		if childTyp.Kind() == reflect.Ptr {
			childTyp = childTyp.Elem()
		}
		childVal := reflect.New(childTyp)
		subFields := structToFields(childVal, structFieldCustomFn, arrayFieldCustomFn)
		if arrayFieldCustomFn != nil {
			subFields = arrayFieldCustomFn(subFields)
		}
		fs = append(fs, subFields...)
	case reflect.Interface:
		childVal := val.Elem()
		subFields := structToFields(childVal, structFieldCustomFn, arrayFieldCustomFn)
		fs = append(fs, subFields...)

	}
	//去重
	m := make(map[string]bool)
	uFs := Fields{}
	for _, f := range fs {
		docName := f.Name
		if !m[docName] {
			uFs = append(uFs, f)
		}
		m[docName] = true
	}
	return uFs
}

func toLowerFirst(s string) string {
	if s == "" {
		return ""
	}
	runes := []rune(s)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

type StructFieldSource string

const (
	StructFieldSource_StructAttr StructFieldSource = "structAttr"
	StructFieldSource_JsonTag    StructFieldSource = "jsonTag"
	StructFieldSource_GormTag    StructFieldSource = "gormTag"
)

// MakeFieldsFromStruct 从结构体字段,gorm tag,json tag 生成字段信息 主要用于新增记录场景(大表字段太多，直接复用query 的model)
func MakeFieldsFromStruct(m any, source StructFieldSource, columnConfigs ...ColumnConfig) (fs Fields) {
	if m == nil {
		return fs
	}
	val := reflect.Indirect(reflect.ValueOf(m))
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Struct:
		for i := range typ.NumField() {
			attr := typ.Field(i)
			fieldValue := val.Field(i).Interface()
			fieldName := ""
			switch source {
			case StructFieldSource_StructAttr:
				fieldName = toLowerFirst(attr.Name)
			case StructFieldSource_JsonTag:
				jsonTag := attr.Tag.Get("json")
				if jsonTag != "-" {
					fieldName = jsonTag
				}
			case StructFieldSource_GormTag:
				if len(columnConfigs) == 0 {
					err := errors.Errorf("MakeFieldsFromStruct  tableColumns required when source is StructFieldSource_GormTag")
					panic(err)
				}
				dbColumnName := extractGormColumn(attr.Tag)
				if dbColumnName == "" {
					continue
				}
				fieldName = ColumnConfigs(columnConfigs).GetByDbNameMust(dbColumnName).FieldName
			}

			if fieldName == "" {
				continue
			}

			f := &Field{
				Name: fieldName,
				ValueFns: ValueFns{
					ValueFn{
						Layer: Value_Layer_SetValue,
						Fn: func(inputValue any, f *Field, fs ...*Field) (any, error) {
							return fieldValue, nil
						},
					},
				},
			}
			fs.Append(f)
		}
	default:
		err := errors.New("MakeFieldsFromAttrName m require struct type")
		panic(err)
	}
	return fs
}

// 提取字段中的 gorm:"column:xxx" 值
func extractGormColumn(tag reflect.StructTag) string {
	gormTag := tag.Get("gorm")
	// 使用正则匹配 column:<字段名>
	re := regexp.MustCompile(`column:([a-zA-Z0-9_]+)`)
	matches := re.FindStringSubmatch(gormTag)
	if len(matches) == 2 {
		fieldName := matches[1] // 返回字段名
		if fieldName == "-" {
			fieldName = ""
		}
		return fieldName
	}
	return ""
}

// MakeColumnConfigFromStruct 从结构体字段,gorm tag 生成 ColumnConfigs 主要用于简化表配置TableConfig.AddColumns(...) 操作
func MakeColumnConfigFromStruct(m any, dbNameSource StructFieldSource) (columnConfigs ColumnConfigs) {
	if m == nil {
		return columnConfigs
	}
	val := reflect.Indirect(reflect.ValueOf(m))
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Struct:
		for i := range typ.NumField() {
			attr := typ.Field(i)
			fieldName := strings.Trim(attr.Tag.Get("json"), "-")
			if fieldName == "" {
				fieldName = toLowerFirst(attr.Name)
			}
			dbColumnName := ""
			switch dbNameSource {
			case StructFieldSource_GormTag:
				dbColumnName = extractGormColumn(attr.Tag)
			}
			if dbColumnName == "" {
				continue
			}

			columnConfig := newColumnConfig(dbColumnName, fieldName)
			columnConfigs.AddColumns(columnConfig)
		}
	default:
		err := errors.New("MakeColumnConfigFromStruct m require struct type")
		panic(err)
	}

	return columnConfigs
}
