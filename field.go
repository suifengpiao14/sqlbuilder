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

var Time_format = "2006-01-02 15:04:05"

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

func (fns *ValueFns) Reset(subFns ...ValueFn) {
	*fns = make(ValueFns, 0)
	*fns = append(*fns, subFns...)
}

// AppendIfNotFirst 追加到最后,但是不能是第一个,一般用于生成SQL时格式化数据
func (fns *ValueFns) AppendIfNotFirst(subFns ...ValueFn) {
	if len(*fns) == 0 {
		return
	}
	fns.Append(subFns...)
}

func (fns *ValueFns) Value(val any) (value any, err error) {
	value = val
	for _, fn := range *fns {
		if fn == nil {
			continue
		}
		value, err = fn(value) //格式化值
		if err != nil {
			return value, err
		}
	}
	return value, nil
}

func ValueFnWhereLike(val any) (value any, err error) {
	str := cast.ToString(val)
	if str == "" {
		return val, nil
	}
	value = Ilike{"%", str, "%"}
	return value, nil
}

type DocNameFn func(name string) string

type DocNameFns []DocNameFn

func (docFns *DocNameFns) Append(fns ...DocNameFn) {
	if *docFns == nil {
		*docFns = make(DocNameFns, 0)
	}
	*docFns = append(*docFns, fns...)
}

func (docFns DocNameFns) Value(name string) string {
	for _, fn := range docFns {
		name = fn(name)
	}
	return name
}

type InitFieldFn func(f *Field, fs ...*Field)
type InitFieldFns []InitFieldFn

func (fns InitFieldFns) Apply(f *Field, fs ...*Field) {
	for _, fn := range fns {
		fn(f, fs...)
	}
}

type SceneInit struct {
	Scene        Scene
	InitFieldFns InitFieldFns
}

type SceneInits []SceneInit

func (sceneInits SceneInits) GetByScene(scene Scene) SceneInit {
	for _, s := range sceneInits {
		if scene.IsSame(s.Scene) {
			return s
		}
	}
	return SceneInit{
		Scene: scene,
	}
}

// Append 常规添加
func (sis *SceneInits) Append(sceneInits ...SceneInit) {
	if *sis == nil {
		*sis = make(SceneInits, 0)
	}
	*sis = append(*sis, sceneInits...)

}

type OrderFn func(f *Field, fs ...*Field) (orderedExpressions []exp.OrderedExpression)

// Field 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Field struct {
	Name          string      `json:"name"`
	ValueFns      ValueFns    `json:"-"` // 增加error，方便封装字段验证规则
	WhereFns      ValueFns    `json:"-"` // 当值作为where条件时，调用该字段格式化值，该字段为nil则不作为where条件查询,没有error，验证需要在ValueFn 中进行,数组支持中间件添加转换函数，转换函数在field.GetWhereValue 统一执行
	OrderFn       OrderFn     `json:"-"` // 当值作为where条件时，调用该字段格式化值，该字段为nil则不作为where条件查询,没有error，验证需要在ValueFn 中进行,数组支持中间件添加转换函数，转换函数在field.GetWhereValue 统一执行
	Schema        *Schema     // 可以为空，为空建议设置默认值
	Table         TableI      // 关联表,方便收集Table全量信息
	Api           interface{} // 关联Api对象,方便收集Api全量信息
	scene         Scene       // 场景
	docNameFns    DocNameFns  // 修改文档字段名称
	sceneInitFns  SceneInits  // 场景初始化配置
	tags          []string    // 方便搜索到指定列,Name 可能会更改,tag不会,多个tag,拼接,以,开头
	dbName        string
	selectColumns []any  // 查询时列
	fieldName     string //列名称,方便通过列名称找到列,列名称根据业务取名,比如NewDeletedAtField 取名 deletedAt
}

const (
	Field_tag_pageIndex = "pageIndex" // 标记为pageIndex列
	Field_tag_pageSize  = "pageSize"  //标记为pageSize列
)

// 不复制whereFns，ValueFns
func (f *Field) Copy() (copyF *Field) {
	copyF = &Field{}
	copyF.Name = f.Name
	copyF.ValueFns.Append(f.ValueFns...)
	copyF.WhereFns.Append(f.WhereFns...)
	var schema Schema
	if f.Schema != nil {
		schema = *f.Schema
	}
	copyF.Schema = &schema // 重新给地址
	copyF.Table = f.Table
	copyF.Api = f.Api
	copyF.scene = f.scene
	copyF.sceneInitFns = f.sceneInitFns
	copyF.selectColumns = f.selectColumns
	copyF.dbName = f.dbName
	copyF.tags = f.tags
	copyF.fieldName = f.fieldName
	return copyF
}

// DBName 转换为DB字段,此处增加该,方法方便跨字段设置(如 polygon 设置外接四边形,使用Between)
func (f *Field) DBName() string {
	dbName := f.dbName
	if dbName == "" {
		dbName = f.Name
	}
	return FieldName2DBColumnName(dbName)
}

// DBName 转换为DB字段,此处增加该,方法方便跨字段设置(如 polygon 设置外接四边形,使用Between)
func (f *Field) SetDBName(dbName string) *Field {
	f.dbName = dbName
	return f
}
func (f *Field) SetSelectColumns(columns ...any) *Field {
	f.selectColumns = columns
	return f
}

func (f *Field) Select() (columns []any) {
	if len(f.selectColumns) == 0 { // 默认增加本列名称
		f.selectColumns = []any{
			f.DBName(),
		}
	}
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
	f.MergeSchema(Schema{Title: title})
	return f
}

func (f *Field) Comment(comment string) *Field {
	f.MergeSchema(Schema{Comment: comment})
	return f
}
func (f *Field) SetTag(tag string) *Field {
	if len(f.tags) == 0 {
		f.tags = append(f.tags, tag)
	}
	return f
}

func (f *Field) HastTag(tag string) bool {
	for _, t := range f.tags {
		if strings.EqualFold(t, tag) {
			return true
		}
	}

	return false
}

func (f *Field) AppendEnum(enums ...Enum) *Field {
	f.MergeSchema(Schema{
		Enums: enums,
	})
	return f
}
func (f *Field) SetRequired(required bool) *Field {
	if f.Schema == nil {
		f.Schema = &Schema{}
	}
	f.Schema.Required = required
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
			Minimum:   minimum,
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
		f.Schema.Enums.Append(schema.Enums...)
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
	if schema.Primary {
		f.Schema.Primary = schema.Primary
	}

	if schema.AutoIncrement {
		f.Schema.AutoIncrement = schema.AutoIncrement
	}
	if schema.Unique {
		f.Schema.Unique = schema.Unique
	}
	if schema.ShieldUpdate {
		f.Schema.ShieldUpdate = schema.ShieldUpdate
	}

	return f
}

// LogString 日志字符串格式
func (f *Field) LogString() string {
	title := f.Name
	if f.Schema != nil && f.Schema.Title == "" {
		title = f.Schema.Title
	}
	val, _ := f.GetValue()
	str := cast.ToString(val)
	out := fmt.Sprintf("%s(%s)", title, str)
	return out
}

func (f *Field) WithOptions(options ...OptionFn) *Field {
	OptionFns(options).Apply(f)
	return f
}

func TrimNilField(field *Field, fn func(field *Field)) {
	if field == nil {
		return
	}
	fn(field)
}

type OptionFn func(field *Field)

func (oFn OptionFn) Apply(field *Field) {
	oFn(field)
}

type OptionFns []OptionFn

func (oFns OptionFns) Apply(field *Field) {
	for _, oFn := range oFns {
		oFn(field)
	}
}

type OptionsFn func(fields ...*Field)

// SetScene 设置 Scene 场景，已第一次设置为准，建议在具体使用时设置，增加 insert,update,select 场景，方便针对场景设置，如enums, 下拉选择有全选，新增、修改没有
func (f *Field) SetScene(scene Scene) *Field {
	if f.scene == "" {
		f.scene = scene
	}
	return f
}

// Scene  获取场景
func (f *Field) SceneFn(scene Scene, initFns ...InitFieldFn) *Field {
	f.sceneInitFns = SceneInits{
		{Scene: scene, InitFieldFns: initFns},
	}
	return f
}
func (f *Field) Apply(initFns ...InitFieldFn) *Field {
	InitFieldFns(initFns).Apply(f)
	return f
}
func (f *Field) SceneInsert(initFn InitFieldFn) *Field {
	f.sceneInitFns.Append(SceneInit{
		Scene:        SCENE_SQL_INSERT,
		InitFieldFns: InitFieldFns{initFn},
	})
	return f
}
func (f *Field) SceneUpdate(initFn InitFieldFn) *Field {
	f.sceneInitFns.Append(SceneInit{
		Scene:        SCENE_SQL_UPDATE,
		InitFieldFns: InitFieldFns{initFn},
	})
	return f
}

func (f *Field) SceneSelect(initFn InitFieldFn) *Field {
	f.sceneInitFns.Append(SceneInit{
		Scene:        SCENE_SQL_SELECT,
		InitFieldFns: InitFieldFns{initFn},
	})
	return f
}

// NewField 生成列，使用最简单版本,只需要提供获取值的函数，其它都使用默认配置，同时支持修改（字段名、标题等这些会在不同的层级设置）
func NewField(valueFn ValueFn, options ...OptionFn) (field *Field) {
	field = &Field{}
	field.ValueFns.InsertAsFirst(valueFn)
	OptionFns(options).Apply(field)
	return field
}

var ERROR_VALUE_NIL = errors.New("error value nil")

func IsErrorValueNil(err error) bool {
	return errors.Is(err, ERROR_VALUE_NIL)
}

// ValueFnArgEmptyStr2NilExceptFields 将空字符串值转换为nil值时排除的字段,常见的有 deleted_at 字段,空置代表正常
//var ValueFnArgEmptyStr2NilExceptFields = Fields{}

var GlobalFnValueFns = func(f Field) ValueFns {
	return ValueFns{
		//GlobalValueFnEmptyStr2Nil(f, ValueFnArgEmptyStr2NilExceptFields...), // 将空置转换为nil,代替对数据判断 if v==""{//ignore}  这个函数在全局修改了函数值，出现问题，比较难跟踪，改到每个组件自己处理
		ValueFnDBSchemaFormatType(f), // 在转换为SQL前,将所有数据类型按照DB类型转换,主要是格式化int和string,提升SQL性能，将数据格式改成DB格式，不影响当期调用链，可以作为全局配置
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

func (f *Field) AppendDocNameFn(subFns ...DocNameFn) {
	f.docNameFns.Append(subFns...)
}

func (f Field) DocName() string {
	name := f.docNameFns.Value(f.Name)
	return name
}

func (f Field) DocRequestArg() (doc DocRequestArg) {
	dbSchema := f.Schema
	if dbSchema == nil {
		return doc
	}
	doc = DocRequestArg{
		Name:        f.DocName(),
		Required:    f.Schema.Required,
		AllowEmpty:  dbSchema.AllowEmpty(),
		Title:       dbSchema.Title,
		Type:        "string",
		Format:      dbSchema.Type.String(),
		Default:     cast.ToString(dbSchema.Default),
		Description: dbSchema.Comment,
		Enums:       dbSchema.Enums,
		RegExp:      dbSchema.RegExp,
	}
	return doc
}

func (f Field) DBColumn() (doc *Column, err error) {
	schema := f.Schema
	if schema == nil {
		err = errors.Errorf("dbSchema required ,filed.Name:%s", f.Name)
		return nil, err
	}

	unsigned := schema.Minimum > -1 // 默认为无符号，需要符号，则最小值设置为最大负数即可
	typeMap := map[string]string{
		"int":    "int",
		"string": "string",
	}
	typ := typeMap[schema.Type.String()]
	if typ == "" {
		if schema.Minimum > 0 || schema.Maximum > 0 { // 如果规定了最小值,最大值，默认为整型
			typ = "int"
		} else {
			typ = "string"
		}
	}

	doc = &Column{
		Name:          f.DBName(),
		Comment:       f.Schema.FullComment(),
		Unsigned:      unsigned,
		Type:          typ,
		Default:       schema.Default,
		Enums:         schema.Enums,
		MaxLength:     schema.MaxLength,
		MinLength:     schema.MinLength,
		Maximum:       schema.Maximum,
		Minimum:       schema.Minimum,
		Primary:       schema.Primary,
		AutoIncrement: schema.AutoIncrement,
	}
	return doc, nil
}

func (f *Field) init(fs ...*Field) {
	if f.sceneInitFns != nil {
		sceneInit := f.sceneInitFns.GetByScene(f.scene)
		if sceneInit.InitFieldFns != nil {
			sceneInit.InitFieldFns.Apply(f, fs...)
		}

	}
}

func (f Field) GetValue() (value any, err error) {
	f.ValueFns.InsertAsSecond(func(in any) (any, error) { //插入数据验证
		err = f.Validate(in)
		if err != nil {
			return in, err
		}
		return in, nil
	})
	f.ValueFns.AppendIfNotFirst(GlobalFnValueFns(f)...) // 在最后生成SQL数据时追加格式化数据
	value, err = f.ValueFns.Value(nil)
	if err != nil {
		return value, err
	}
	if IsNil(value) {
		err = ERROR_VALUE_NIL //相比返回 nil,nil; 此处抛出错误，其它地方更容易感知中断处理，如需要继续执行，执行忽略这个类型Error 即可
		return nil, err
	}
	return value, nil
}

// WhereData 获取Where 值
func (f Field) WhereData(fs ...*Field) (value any, err error) {
	f = *f.Copy()
	f.init(fs...)
	if len(f.WhereFns) == 0 {
		return nil, nil
	}
	value, err = f.GetValue()
	if IsErrorValueNil(err) {
		err = nil // 这里不直接返回，仍然遍历 执行whereFns，方便理解流程（直接返回后，期望的whereFn没有执行，不知道流程在哪里中断了，也没有错误抛出，非常困惑，所以不能直接返回）
	}
	if err != nil {
		return value, err
	}
	for _, fn := range f.WhereFns {
		value, err = fn(value) // value 为nil 继续循环，主要考虑调试方便，若中途中断，可能导致调试困难(代码未按照预期运行，不知道哪里中断了)，另外一般调试时，都没有写参数值，方便能快速查看效果
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
	return valueFn(in)
}

// IsEqual 判断名称值是否相等
func (f Field) IsEqual(o Field) bool {
	fv, err := f.GetValue()
	if err != nil || IsNil(fv) {
		return false
	}
	ov, err := o.GetValue()
	if err != nil || IsNil(ov) {
		return false
	}
	return strings.EqualFold(cast.ToString(fv), cast.ToString(ov)) && strings.EqualFold(f.Name, o.Name)
}

func (c Field) Validate(val any) (err error) {
	if c.Schema == nil {
		return nil
	}
	if IsNil(val) {
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
	switch c.Schema.Type {
	case Schema_Type_string:
		value = cast.ToString(value)
	case Schema_Type_json:
		b, _ := json.Marshal(value)
		value = string(b)
	case Schema_Type_int:
		value = cast.ToInt(value)
	}

	return value
}

func (f Field) Data(fs ...*Field) (data any, err error) {
	f = *f.Copy() // 复制一份,不影响其它场景
	f.init(fs...)
	val, err := f.GetValue()
	if IsErrorValueNil(err) {
		return nil, nil // 忽略空值错误
	}
	if err != nil {
		return nil, err
	}
	if f.Schema != nil && f.Schema.ShieldUpdate {
		return nil, nil
	}
	data = map[string]any{
		f.DBName(): val,
	}
	return data, nil
}

func (f Field) Where(fs ...*Field) (expressions Expressions, err error) {
	val, err := f.WhereData(fs...)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	if ex, ok := TryParseExpressions(f.DBName(), val); ok {
		return ex, nil
	}
	return ConcatExpression(goqu.Ex{f.DBName(): val}), nil
}

func (f Field) Order(fs ...*Field) (orderedExpressions []exp.OrderedExpression) {
	orderedExpressions = make([]exp.OrderedExpression, 0)
	if f.OrderFn != nil {
		ex := f.OrderFn(&f, fs...)
		orderedExpressions = append(orderedExpressions, ex...)
	}
	return orderedExpressions
}

type Fields []*Field

func (fs Fields) Copy() (fields Fields) {
	fields = make(Fields, 0)
	for _, f := range fs {
		fields = append(fields, f.Copy())
	}
	return fields
}

func (fs Fields) SetScene(scene Scene) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SetScene(scene)
	}
	return fs
}
func (fs Fields) SceneInsert(fn InitFieldFn) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SceneInsert(fn)
	}
	return fs
}

func (fs Fields) SceneUpdate(fn InitFieldFn) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].SceneUpdate(fn)
	}
	return fs
}

func (fs Fields) SceneSelect(fn InitFieldFn) Fields {
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

func (fs Fields) Pagination() (index int, size int) {
	if pageIndex, ok := fs.GetByTag(Field_tag_pageIndex); ok {
		val, _ := pageIndex.GetValue()
		index = cast.ToInt(val)

	}
	if pageSize, ok := fs.GetByTag(Field_tag_pageSize); ok {
		val, _ := pageSize.GetValue()
		size = cast.ToInt(val)
	}

	return index, size
}

func NewFields(fields ...*Field) *Fields {
	return new(Fields).Append(fields...)
}

func (fs Fields) Contains(field Field) (exists bool) {
	for _, f := range fs {
		if strings.EqualFold(f.Name, field.Name) { // 暂时值判断名称,后续根据需求,再增加类型
			return true
		}
	}
	return false
}

func (fs *Fields) Append(fields ...*Field) *Fields {
	*fs = append(*fs, fields...)
	return fs
}

func (fs Fields) WithOptions(options ...OptionsFn) Fields {
	for _, optionFn := range options {
		optionFn(fs...)
	}
	return fs
}

func (fs Fields) Apply(fns ...InitFieldFn) Fields {
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
func (fs Fields) AppendDocNameFn(docNameFns ...DocNameFn) Fields {
	for i := 0; i < len(fs); i++ {
		fs[i].docNameFns.Append(docNameFns...)
	}
	return fs
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
		val, _ := f.GetValue()
		m[FieldName2DBColumnName(f.Name)] = val
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
func (fs Fields) GetByFieldName(fieldName string) (f *Field, ok bool) {
	for i := 0; i < len(fs); i++ {
		if strings.EqualFold(fieldName, f.fieldName) {
			return f, true
		}
	}
	return nil, false
}

// DocRequestArgs 生成文档请求参数部分
func (fs Fields) DocRequestArgs() (args DocRequestArgs) {
	args = make(DocRequestArgs, 0)
	for _, f := range fs {
		args = append(args, f.DocRequestArg())
	}
	return args
}

func (fs Fields) DBNames() (dbNames []string, err error) {
	dbNames = make([]string, 0)
	for _, f := range fs {
		dbNames = append(dbNames, f.DBName())
	}
	return dbNames, nil
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

func (fs Fields) Order() (orderedExpressions []exp.OrderedExpression) {
	orderedExpressions = make([]exp.OrderedExpression, 0)
	for _, field := range fs {
		subExprs := field.Order(fs...)
		orderedExpressions = append(orderedExpressions, subExprs...)
	}
	return orderedExpressions
}

func (fs Fields) Data() (data any, err error) {
	dataMap := make(map[string]any, 0)
	for _, f := range fs {
		data, err := f.Data(fs...)
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
	return dataMap, nil
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

type Neq any

func TryNeq(field string, value any) (expressions Expressions, ok bool) {
	if val, ok := value.(Neq); ok {
		identifier := goqu.C(field)
		return ConcatExpression(identifier.Neq(val)), true
	}
	return nil, false
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

// Between 介于2者之间(包含上下边界，对于不包含边界情况，可以修改值范围或者直接用表达式),3个元素时为: col1<12<col2 格式
type Between [3]any

func TryConvert2Betwwen(field string, value any) (expressions Expressions, ok bool) {
	if between, ok := value.(Between); ok {
		identifier := goqu.C(field)
		min, val, max := between[0], between[1], between[2]

		if max != nil {
			expressions = ConcatExpression(goqu.L("?", val).Between(exp.NewRangeVal(goqu.C(cast.ToString(min)), goqu.C(cast.ToString(max)))))
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
