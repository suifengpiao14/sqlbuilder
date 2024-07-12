package sqlbuilder

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/suifengpiao14/funcs"
)

var Time_format = "2024-01-02 15:04:05"

// type ValueFnfunc(in any) (value any,err error) //函数签名返回参数命名后,容易误导写成 func(in any) (value any,err error){return value,nil};  正确代码:func(in any) (value any,err error){return in,nil};
type ValueFn func(in any) (any, error) // 函数之所有接收in 入参，有时模型内部加工生成的数据需要存储，需要定制格式化，比如多边形产生的边界框4个点坐标

type ValueFns []ValueFn

// Insert 追加元素,不建议使用,建议用InsertAsFirst,InsertAsSecond
func (fns *ValueFns) Insert(index int, subFns ...ValueFn) {
	if *fns == nil {
		*fns = make(ValueFns, 0)
	}
	l := len(*fns)
	if l == 0 || index < 0 || l <= index { // 本身没有,直接添加,或者计划添加到结尾,或者指定位置比现有数组长,直接追加
		*fns = append(*fns, subFns...)
		return
	}
	if index == 0 { // index =0 插入第一个
		tmp := make(ValueFns, 0)
		tmp = append(tmp, subFns...)
		tmp = append(tmp, *fns...)
		*fns = tmp
		return
	}
	pre, after := (*fns)[:index], (*fns)[index:]
	tmp := make(ValueFns, 0)
	tmp = append(tmp, pre...)
	tmp = append(tmp, subFns...)
	tmp = append(tmp, after...)
	*fns = tmp
}

// InsertAsFirst 作为第一个元素插入,一般用于将数据导入到whereFn 中
func (fns *ValueFns) InsertAsFirst(subFns ...ValueFn) {
	fns.Insert(0, subFns...)
}

// InsertAsSecond 作为第二个元素插入,一般用于在获取数据后立即验证器插入
func (fns *ValueFns) InsertAsSecond(subFns ...ValueFn) {
	fns.Insert(1, subFns...)
}

// Append 常规添加
func (fns *ValueFns) Append(subFns ...ValueFn) {
	fns.Insert(-1, subFns...)
}

// AppendIfNotFirst 追加到最后,但是不能是第一个,一般用于生成SQL时格式化数据
func (fns *ValueFns) AppendIfNotFirst(subFns ...ValueFn) {
	if len(*fns) == 0 {
		return
	}
	fns.Append(subFns...)
}

func NewValueFns(fn func() (value any, err error)) ValueFns {
	return ValueFns{
		func(in any) (value any, err error) {
			return fn()
		},
	}
}

type WhereValueFn func(dbColumnName string, data any) (any, error) // whereFn 中会使用到数据库字段名 需要将dbColumnName传入

type WhereValueFns []WhereValueFn

// Insert 追加元素,不建议使用,建议用InsertAsFirst,InsertAsSecond
func (fns *WhereValueFns) Insert(index int, subFns ...WhereValueFn) {
	if *fns == nil {
		*fns = make(WhereValueFns, 0)
	}
	l := len(*fns)
	if l == 0 || index < 0 || l <= index { // 本身没有,直接添加,或者计划添加到结尾,或者指定位置比现有数组长,直接追加
		*fns = append(*fns, subFns...)
		return
	}
	if index == 0 { // index =0 插入第一个
		tmp := make(WhereValueFns, 0)
		tmp = append(tmp, subFns...)
		tmp = append(tmp, *fns...)
		*fns = tmp
		return
	}
	pre, after := (*fns)[:index], (*fns)[index:]
	tmp := make(WhereValueFns, 0)
	tmp = append(tmp, pre...)
	tmp = append(tmp, subFns...)
	tmp = append(tmp, after...)
	*fns = tmp
}

// InsertAsFirst 作为第一个元素插入,一般用于将数据导入到whereFn 中
func (fns *WhereValueFns) InsertAsFirst(subFns ...WhereValueFn) {
	fns.Insert(0, subFns...)
}

// InsertAsSecond 作为第二个元素插入,一般用于在获取数据后立即验证器插入
func (fns *WhereValueFns) InsertAsSecond(subFns ...WhereValueFn) {
	fns.Insert(1, subFns...)
}

// Append 常规添加
func (fns *WhereValueFns) Append(subFns ...WhereValueFn) {
	fns.Insert(-1, subFns...)
}

// AppendIfNotFirst 追加到最后,但是不能是第一个,一般用于生成SQL时格式化数据
func (fns *WhereValueFns) AppendIfNotFirst(subFns ...WhereValueFn) {
	if len(*fns) == 0 {
		return
	}
	fns.Append(subFns...)
}

func NewWhereValueFns(fn func() (value any, err error)) WhereValueFns {
	return WhereValueFns{
		func(filedName string, data any) (value any, err error) {
			return fn()
		},
	}
}

// ValueFnShield 屏蔽值，常用于取消某个字段作为查询条件
func WhereValueFnShield(dbColumnName string, val any) (value any, err error) {
	return nil, nil
}

// Field 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Field struct {
	Name        string                                                 `json:"name"`
	ValueFns    ValueFns                                               `json:"-"` // 增加error，方便封装字段验证规则
	WhereFns    WhereValueFns                                          `json:"-"` // 当值作为where条件时，调用该字段格式化值，该字段为nil则不作为where条件查询,没有error，验证需要在ValueFn 中进行,数组支持中间件添加转换函数，转换函数在field.GetWhereValue 统一执行
	Migrate     func(table string, options ...MigrateOptionI) Migrates `json:"-"`
	ValidateFns ValidateFns                                            `json:"-"` // 设置验证参数验证器
	Schema      *Schema                                                // 可以为空，为空建议设置默认值
	Table       TableI                                                 // 关联表,方便收集Table全量信息
	Api         interface{}                                            // 关联Api对象,方便收集Api全量信息
}

func (f *Field) AppendValidateFn(fns ...ValidateFn) *Field {
	if f.ValidateFns == nil {
		f.ValidateFns = make(ValidateFns, 0)
	}
	addr := &f.ValidateFns
	*addr = append(*addr, fns...)
	return f
}

func (f *Field) SetName(name string) *Field {
	f.Name = name
	return f
}
func (f *Field) SetTitle(title string) *Field {
	schema := Schema{}
	schema.Title = title
	f.MergeSchema(schema)
	return f
}

func (f *Field) MergeSchema(schema Schema) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}

	if schema.Title != "" {
		f.Schema.Title = schema.Title
	}
	if schema.Required {
		f.Schema.Required = schema.Required
	}

	if schema.Comment != "" {
		f.Schema.Comment = schema.Comment
	}
	if schema.Type != "" {
		f.Schema.Type = schema.Type
	}
	if schema.Default != "" {
		f.Schema.Default = schema.Default
	}

	if len(schema.Enums) > 0 {
		f.Schema.Enums = schema.Enums
	}

	if schema.MaxLength > 0 {
		f.Schema.MaxLength = schema.MaxLength
	}

	if schema.MinLength > 0 {
		f.Schema.MinLength = schema.MinLength
	}

	if schema.Maximum > 0 {
		f.Schema.Maximum = schema.Maximum
	}

	if schema.Minimum > 0 {
		f.Schema.Minimum = schema.Minimum
	}

	if schema.RegExp != "" {
		f.Schema.RegExp = schema.RegExp
	}

	return f
}

// LogString 日志字符串格式
func (f *Field) LogString() string {
	title := f.Name
	if f.Schema != nil && f.Schema.Title == "" {
		title = f.Schema.Title
	}
	val, _ := f.GetValue(nil)
	str := cast.ToString(val)
	out := fmt.Sprintf("%s(%s)", title, str)
	return out
}

// NewField 生成列，使用最简单版本,只需要提供获取值的函数，其它都使用默认配置，同时支持修改（字段名、标题等这些会在不同的层级设置）
func NewField(valueFn ValueFn) (field *Field) {
	field = &Field{}
	field.ValueFns.InsertAsFirst(valueFn)
	return field
}

var ERROR_VALUE_NIL = errors.New("error value nil")

func IsErrorValueNil(err error) bool {
	return errors.Is(err, ERROR_VALUE_NIL)
}

// ValueFnArgEmptyStr2NilExceptFields 将空字符串值转换为nil值时排除的字段,常见的有 deleted_at 字段,空置代表正常
var ValueFnArgEmptyStr2NilExceptFields = Fields{}

var GlobalFnValueFns = func(f Field) ValueFns {
	return ValueFns{
		ValueFnEmptyStr2Nil(f, ValueFnArgEmptyStr2NilExceptFields...), // 将空置转换为nil,代替对数据判断 if v==""{//ignore}
		ValueFnDBSchemaFormatType(f),                                  // 在转换为SQL前,将所有数据类型按照DB类型转换,主要是格式化int和string,提升SQL性能
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

func (f Field) DocRequestArg() (doc *DocRequestArg, err error) {
	dbSchema := f.Schema
	if dbSchema == nil {
		err = errors.Errorf("dbSchema required ,filed.Name:%s", f.Name)
		return nil, err
	}
	doc = &DocRequestArg{
		Name:        f.Name,
		Required:    f.Schema.Required,
		AllowEmpty:  dbSchema.AllowEmpty(),
		Title:       dbSchema.Title,
		Type:        "string",
		Format:      dbSchema.Type.String(),
		Default:     cast.ToString(dbSchema.Default),
		Description: dbSchema.Comment,
		Enums:       make(Enums, 0),
		RegExp:      dbSchema.RegExp,
	}
	return doc, nil
}

func (f Field) DBColumn() (doc *Column, err error) {
	schema := f.Schema
	if schema == nil {
		err = errors.Errorf("dbSchema required ,filed.Name:%s", f.Name)
		return nil, err
	}

	unsigned := schema.Minimum > -1 // 默认为无符号，需要符号，则最小值设置为最大负数即可
	typeMap := map[string]string{}
	typ := typeMap[schema.Type.String()]
	if typ == "" {
		if schema.Minimum > 0 || schema.Maximum > 0 { // 如果规定了最小值,最大值，默认为整型
			typ = "int"
		} else {
			typ = "string"
		}
	}

	doc = &Column{
		Name:      FieldName2DBColumnName(f.Name),
		Comment:   f.Schema.FullComment(),
		Unsigned:  unsigned,
		Type:      typ,
		Default:   schema.Default,
		Enums:     schema.Enums,
		MaxLength: schema.MaxLength,
		MinLength: schema.MinLength,
		Maximum:   schema.Maximum,
		Minimum:   schema.Minimum,
	}
	return doc, nil
}

func (f Field) GetValue(in any) (value any, err error) {
	value = in
	f.ValueFns.InsertAsSecond(func(in any) (any, error) { //插入数据验证
		err = f.Validate(in)
		if err != nil {
			return in, err
		}
		return in, nil
	})
	f.ValueFns.AppendIfNotFirst(GlobalFnValueFns(f)...) // 在最后生成SQL数据时追加格式化数据
	for _, fn := range f.ValueFns {
		value, err = fn(value) //格式化值
		if err != nil {
			return value, err
		}
	}
	if IsNil(value) {
		err = ERROR_VALUE_NIL //相比返回 nil,nil; 此处抛出错误，其它地方更容易感知中断处理，如需要继续执行，执行忽略这个类型Error 即可
		return nil, err
	}
	return value, nil
}

// GetWhereValue 获取Where 值
func (f Field) GetWhereValue(in any) (value any, err error) {
	if len(f.WhereFns) == 0 {
		return nil, nil
	}
	value = in
	value, err = f.GetValue(in)
	if IsErrorValueNil(err) {
		err = nil
		return nil, nil
	}
	if err != nil {
		return value, err
	}

	fieldName := FieldName2DBColumnName(f.Name)
	for _, fn := range f.WhereFns {
		value, err = fn(fieldName, value)
		if err != nil {
			return value, err
		}
	}
	if IsNil(value) {
		return nil, nil
	}

	return value, nil
}

// IsEqual 判断名称值是否相等
func (f Field) IsEqual(o Field) bool {
	fv, err := f.GetValue(nil)
	if err != nil || IsNil(fv) {
		return false
	}
	ov, err := o.GetValue(nil)
	if err != nil || IsNil(ov) {
		return false
	}
	return strings.EqualFold(cast.ToString(fv), cast.ToString(ov)) && strings.EqualFold(f.Name, o.Name)
}

// Validate  实现ValidateI 接口 可以再 valueFn ,whereValueFn 中手动调用
func (c Field) Validate(val any) (err error) {
	for _, vFn := range c.ValidateFns {
		err = vFn.Validate(val)
		if err != nil {
			return err
		}
	}
	if c.Schema == nil {
		return nil
	}
	rv := reflect.Indirect(reflect.ValueOf(val))
	err = c.Schema.Validate(c.Name, rv)
	if err != nil {
		return err
	}

	return
}

func (c Field) FormatType(val any) (value any) {
	value = val
	if c.Schema == nil {
		return value
	}
	switch c.Schema.Type {
	case Schema_Type_string:
		value = cast.ToString(value)
	case Schema_Type_int:
		value = cast.ToInt(value)
	}

	return value
}

func (f Field) Data() (data any, err error) {
	val, err := f.GetValue(nil)
	if IsErrorValueNil(err) {
		return nil, nil // 忽略空值错误
	}
	if err != nil {
		return nil, err
	}
	data = map[string]any{
		FieldName2DBColumnName(f.Name): val,
	}
	return data, nil
}

func (f Field) Where() (expressions Expressions, err error) {
	val, err := f.GetWhereValue(nil)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if ex, ok := TryParseExpressions(FieldName2DBColumnName(f.Name), val); ok {
		return ex, nil
	}
	return ConcatExpression(goqu.Ex{FieldName2DBColumnName(f.Name): val}), nil
}

type Fields []Field

func (fs Fields) Contains(field Field) (exists bool) {
	for _, f := range fs {
		if strings.EqualFold(f.Name, field.Name) { // 暂时值判断名称,后续根据需求,再增加类型
			return true
		}
	}
	return false
}

func (fs Fields) Where() (expressions Expressions, err error) {
	expressions = make(Expressions, 0)
	for _, field := range fs {
		subExprs, err := field.Where()
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, subExprs...)
	}
	return expressions, nil
}

func (fs Fields) Json() string {
	b, _ := json.Marshal(fs)
	return string(b)
}
func (fs Fields) String() string {
	m := make(map[string]any)
	for _, f := range fs {
		val, _ := f.GetValue(nil)
		m[FieldName2DBColumnName(f.Name)] = val
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// DocRequestArgs 生成文档请求参数部分
func (fs Fields) DocRequestArgs() (args DocRequestArgs, err error) {
	args = make(DocRequestArgs, 0)
	for _, f := range fs {
		arg, err := f.DocRequestArg()
		if err != nil {
			return nil, err
		}
		args = append(args, *arg)
	}
	return args, nil
}
func (fs Fields) DBColumns() (columns Columns, err error) {
	columns = make(Columns, 0)
	for _, f := range fs {
		column, err := f.DBColumn()
		if err != nil {
			return nil, err
		}
		columns = append(columns, *column)
	}
	return columns, nil
}

func (fs Fields) Data() (data any, err error) {
	dataSet := make(DataSet, 0)
	for _, f := range fs {
		dataSet = append(dataSet, DataFn(f.Data))
	}
	return MergeData(dataSet...)
}

type FieldFn func() []Field

func (fn FieldFn) Data() (data any, err error) {
	dataSet := make(DataSet, 0)
	columns := fn()
	for _, c := range columns {
		dataSet = append(dataSet, DataFn(c.Data))
	}
	return MergeData(dataSet...)
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

// Ilike 不区分大小写like语句
type Ilike [3]any

func TryIlike(field string, value any) (expressions Expressions, ok bool) {
	if iLike, ok := value.(Ilike); ok {
		identifier := goqu.C(field)
		strArr := make([]string, 0)
		for _, arg := range iLike {
			strArr = append(strArr, cast.ToString(arg))
		}
		val := strings.Join(strArr, "")
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

// Between 介于2者之间(包含上下边界，对于不包含边界情况，可以修改值范围或者直接用表达式)
type Between [2]any

func TryConvert2Betwwen(field string, value any) (expressions Expressions, ok bool) {
	if between, ok := value.(Between); ok {
		identifier := goqu.C(field)
		min, max := between[0], between[1]
		if !IsNil(min) && !IsNil(max) {
			expressions = append(expressions, identifier.Between(exp.NewRangeVal(min, max)))
			return expressions, true
		}
		if !IsNil(min) {
			return ConcatExpression(identifier.Gte(min)), true
		}

		if !IsNil(max) {
			return ConcatExpression(identifier.Lte(min)), true
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
	return nil, false
}
func Expression2String(expressions ...goqu.Expression) string {
	sql, _, _ := Dialect.Select().Where(expressions...).ToSQL()
	return sql
}

// FieldName2DBColumnName 将接口字段转换为数据表字段列名称
var FieldName2DBColumnName = func(fieldName string) (dbColumnName string) {
	dbColumnName = funcs.ToSnakeCase(fieldName)
	dbColumnName = fmt.Sprintf("F%s", strings.TrimPrefix(dbColumnName, "f")) // 增加F前缀
	return dbColumnName
}
