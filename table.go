package sqlbuilder

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/pkg/errors"
	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/memorytable"
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

func (id DBIdentifier) NameWithQuotes() string {
	name := id.BaseName()
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

type TableView interface {
	TableView() TableConfig
}

type TableConfig struct {
	DBName
	Columns                  ColumnConfigs            // 后续吧table 纳入，通过 Column.Identity 生成 Field 操作
	FieldName2DBColumnNameFn FieldName2DBColumnNameFn `json:"-"`
	Schema                   SchemaConfig
	_handler                 Handler // 内部获取，使用GetHandler方法（GetHander 方法挂载一些初始化动作）
	Comment                  string  // 表注释
	Indexs                   Indexs  // 索引信息，唯一索引，在新增时会自动校验是否存在,更新时会自动保护
	// 表级别的字段（值产生方式和实际数据无关），比如创建时间、更新时间、删除字段等，这些字段设置好后，相关操作可从此获取字段信息,增加该字段，方便封装delete操作、冗余字段自动填充等操作, 增加ctx 入参 方便使用ctx专递数据，比如 业务扩展多租户，只需表增加相关字段，在ctx中传递租户信息，并设置表级别字段场景即可
	// 比如规则模型，表中cityId,classId,productId 字段只是方便后台查询设置,api 侧只需要规则表达式(expresson)即可,expresson 往往是其它字段按照按照需求生成的字符串,
	//此时使用hook确保关注的字段发生变化时，自动更新冗余数据(在构造sql 的Fields 内追加冗余字段Field)
	//Deprecated: 废弃hook,hook只能修改写入数据库前,无法在操作数据库之后再修改. 包内引用了middleware概念,可以使用middleware统一实现
	tableLevelFieldsHook HookFn
	modelMiddlewares     ModelMiddlewares
	shardedTableNameFn   func(fs ...Field) (shardedTableNames []string) // 分表策略，比如按时间分表，此处传入字段信息，返回多个表名
	//publisher            message.Publisher table 只和gochannel publisher 交互，不直接和外部交互，如果需要发布到外部(如mq,kafka等)时，监听内部gochannel 转发即可，这样设计的目的是将领域内事件和领域外事件分离，方便内聚和聚合
	comsumerMakers []func(table TableConfig) Consumer // 当前表级别的消费者(主要用于在表级别同步数据)
	//views          TableConfigs view概念没有用 table在这里不是一等公民,Field才是一等公民,view功能通过FieldsI 接口实现,并且更合适
	topicRouteKey string       // 2025-10-25 事件必须在运行表上发布,原因:接受事件后需要知道运行表是哪个,方便获取数据.所以运行表一定要获取,既然这样,将topic挂在运行表上就很合适. 为解决中间件订阅互不影响,所以增加routeKey类似mq的消息路由
	topicTables   TableConfigs // 订阅的主题表，key 为固定的TableConfig.programTableName（这个字段可能无效，应为各模型订阅事件，topic是固定的2025-10-24）

}

type HookFn func(ctx context.Context, scene Scene) (hookedFields Fields)

// WithFieldHook 数据变更时，自动填充冗余字段, 比如品类id集合发生变化时，自动更新品类id集合等操作
func (t TableConfig) WithFieldHook(hooks HookFn) TableConfig {
	t.tableLevelFieldsHook = hooks
	return t
}
func (t TableConfig) WithModelMiddlewares(middlewares ...ModelMiddleware) TableConfig {
	t.modelMiddlewares = append(t.modelMiddlewares, middlewares...)
	return t
}

func (t TableConfig) WithTopicRouteKey(topicRouteKey string) TableConfig {
	t.topicRouteKey = topicRouteKey
	return t
}

func (t TableConfig) WithTopicTable(topicTables ...TableConfig) TableConfig { // 订阅其它表的变更事件，并同步到当前表中
	t.topicTables = append(t.topicTables, topicTables...)
	return t
}

// GetTopicTable 通过模型表查询实际订阅表(由于表、表对应service 有依赖关系，订阅表往往和程序依赖关系相反，导致循环依赖语法错误，但是订阅代码确实属于表层面，所以增加自动匹配订阅表函数，后续观察和优化)
func (t TableConfig) GetTopicTable(modelTable TableConfig) (publishTable TableConfig, err error) {
	for _, topicTable := range t.topicTables {
		err := topicTable.CheckMissOutFieldName(modelTable)
		if err == nil {
			return topicTable, nil
		}
	}
	err = errors.Errorf("模型表(%s)不在表(%s)订阅表集合中", modelTable.Name, t.Name)
	return publishTable, err
}

func (t TableConfig) WithConsumerMakers(consumerMakers ...func(table TableConfig) (consumer Consumer)) TableConfig { // 使用tableGetter 能延迟获取table，主要是等待 handler 初始化完毕
	t.comsumerMakers = consumerMakers
	return t
}

func (t TableConfig) SubscribeMaker(consumerMakers ...func(table TableConfig) (consumer Consumer)) TableConfig {
	t.WithConsumerMakers(consumerMakers...)
	return t
}

// AddViews 别名配置的columns 必须被完整包含，否则会panic, 主要用于不同模型映射同一张运行表(应用封装的package 必备入口)
/*
func (t *TableConfig) AddViews(views ...TableConfig) (err error) {
	//别名配置的columns 必须被完整包含
	for _, aliaTableConfig := range views {
		fieldNames := aliaTableConfig.Columns.Fields().Names()
		err := t.Columns.CheckMissOutFieldName(fieldNames...)
		if err != nil {
			err = errors.WithMessagef(err, "缺失别名表(%s)配置字段", aliaTableConfig.Name)
			return err
		}
		t.AddIndexs(aliaTableConfig.Indexs...)
	}
	//t.views = append(t.views, views...)
	return nil
}
*/
/*
func (t TableConfig) GetColumnsWithViewColumns() (columnConfigs ColumnConfigs) {
	columnConfigs = append(columnConfigs, t.Columns...)
	for _, view := range t.views {
		columnConfigs = append(columnConfigs, view.Columns...)
	}
	return columnConfigs
}
*/

func (t TableConfig) CheckMissOutFieldName(modelTable TableConfig) (err error) { //校验当前表是否包含给定表所包含的模型
	return t.Columns.CheckMissOutFieldName(modelTable.Columns.Fields().FilterByModelRequired().Names()...)
}

// GetConsumerMakers 获取订阅者制造器(制造者一般会封装到package内部，使用方需要复制maker，然后在具体运行时启动订阅者，所以这里提供获取已经封装好的制造者)
func (t TableConfig) GetConsumerMakers() []func(table TableConfig) (consumer Consumer) {
	return t.comsumerMakers
}

var tableInitMark sync.Map

func initTableOnce(table TableConfig, initFn func() (err error)) (err error) {
	_, ok := tableInitMark.LoadOrStore(table.Name, true)
	if ok { //已经初始化过
		return nil
	}
	err = initFn()
	if err != nil {
		tableInitMark.Delete(table.Name)
	}
	return err

}

func (t TableConfig) Init() (err error) { //init 会挂载在 t.GetHandler 方法中，会多次调用，所以需要确保只执行一次
	if t._handler == nil {
		err := errors.New("TableConfig.handler 未初始化,请先初始化handler再启用消费者监听")
		return err
	}
	//每个表(表名称) 只初始化一次，防止不同实例重复启动订阅者(重复启动订阅者会导致重复消费问题)
	err = initTableOnce(t, func() (err error) {
		for _, consumerMaker := range t.comsumerMakers {
			subscriber := consumerMaker(t)
			err = StartSubscriberOnce(t.GetTopic(), subscriber)
			if err != nil {
				return err
			}
		}

		return err
	})
	return err
}

// func (t *TableConfig) SetPubSubLoger(log watermill.LoggerAdapter) {
// 	t.pubSubLoger = log
// }

// func (t *TableConfig) getPubSubLoger() watermill.LoggerAdapter {
// 	if t.pubSubLoger != nil {
// 		return t.pubSubLoger
// 	}
// 	t.pubSubLoger = MessageLogger
// 	return t.pubSubLoger

// }

func (t *TableConfig) GetPublisher() message.Publisher {
	return GetPublisher(t.GetTopic())
}

func (t TableConfig) GetTopic() string {
	topic := fmt.Sprintf("topic_table_%s_routeKey_%s", t.Name, t.topicRouteKey) // 这地方目前是兼容历史，后续t.topc 比定不能为空
	return topic
}

// func (t TableConfig) WithTopic(topic string) TableConfig { // 有时候topic需要固定为封装模块内置的表，所以此处支持自定义topic，一般是用内置表topic赋值给当前表
// 	t.topic = topic
// 	return t
// }

// // GetConsumer 获取订阅者，方便后续扩展，比如增加订阅者持久化等操作
//
//	func (t TableConfig) GetConsumer() message.Subscriber {
//		return t.getPubSub()
//	}
//
// Publish
func (t TableConfig) Publish(event EventMessage) (err error) {
	var pubSub = t.GetPublisher()
	msg, err := event.ToMessage()
	if err != nil {
		return err
	}
	err = pubSub.Publish(t.GetTopic(), msg)
	if err != nil {
		return err
	}
	return nil
}

// SubscriberAggregation 订阅消息，这个方法没有什么功能，只是提倡将订阅者关联到表上面，只是表明这些订阅归属于当前表
// func (t TableConfig) SubscriberAggregation(subscriberMakers ...func() Subscriber) (err error) {
// 	for i := range subscriberMakers {
// 		subscriber := subscriberMakers[i]()
// 		subscriber.Consume()
// 	}
// 	return nil
// }

// SubscribeSelfTopic 订阅消息，这个方法没有什么功能，只是提倡为订阅当前表消息的消费者做代理，减少使用者代码量，经常在package为订阅领域内部表事件的消费者做代理
// 使得package使用更简洁
// func (t TableConfig) SubscribeSelfTopic(subscriberMakers ...func() Subscriber) (err error) {
// 	subscriberMakersWithSelfTopic := make([]func() Subscriber, 0)
// 	for i := range subscriberMakers {
// 		subscriber := subscriberMakers[i]()
// 		if subscriber.SubscriberFn == nil {
// 			err = errors.New("订阅者函数不能为空")
// 			return err
// 		}
// 		subscriber.Consumer = t.GetConsumer()
// 		subscriber.Topic = t.GetTopic()
// 		if subscriber.Logger == nil {
// 			subscriber.Logger = t.getPubSubLoger()
// 		}
// 		subscriberMakersWithSelfTopic = append(subscriberMakersWithSelfTopic, func() Subscriber { return subscriber })
// 	}
// 	err = t.SubscriberAggregation(subscriberMakersWithSelfTopic...)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func NewTableConfig(name string) TableConfig {
	t := TableConfig{
		DBName: DBName{Name: name},
		tableLevelFieldsHook: func(ctx context.Context, scene Scene) (hookedFields Fields) {
			return
		},
	}
	return t
}

// Repository 封装了基本的增删改查操作，方便使用(这里表明repository 属于table层面,不属于业务层面，repository可以对应多个业务模型)

func (t TableConfig) Repository() Repository {
	return NewRepository(t)
}

func (t TableConfig) WithTableName(name string) TableConfig {
	t.DBName = DBName{Name: name}
	return t
}

func (t TableConfig) WithComment(comment string) TableConfig {
	t.Comment = comment
	return t
}
func (t TableConfig) WithShardedTableNameFn(shardedTableNameFn func(fs ...Field) (shardedTableNames []string)) TableConfig {
	t.shardedTableNameFn = shardedTableNameFn
	return t
}
func (t TableConfig) getShardedTableNames(fs ...Field) (shardedTableNames []string) {
	if t.shardedTableNameFn == nil {
		return nil
	}
	shardedTableNames = t.shardedTableNameFn(fs...)
	return shardedTableNames
}
func (t TableConfig) isShardedTable() (isShardedTable bool) {
	isShardedTable = t.shardedTableNameFn != nil
	return isShardedTable
}

func (t TableConfig) AddColumns(columns ...ColumnConfig) TableConfig {
	allCols := make(ColumnConfigs, 0)
	for _, c := range columns {
		allCols = append(allCols, c)
		if c.field != nil {
			for _, aliaField := range c.field.alias {
				aliaCol := c.Copy()
				aliaCol.FieldName = aliaField.Name
				aliaCol.field = aliaField
				allCols = append(allCols, aliaCol)
			}
		}
	}
	t.Columns.AddColumns(allCols...)
	return t
}
func (t TableConfig) InitDDLSort() TableConfig {
	t.Columns = t.Columns.InitDDLSort()
	return t
}

func (t TableConfig) WalkColumn(walkColumnFn func(columnConfig ColumnConfig) ColumnConfig) TableConfig {
	t.Columns.WalkColumn(walkColumnFn)
	return t
}

func (t TableConfig) AddIndexs(indexs ...Index) TableConfig {
	t.Indexs.Append(t, indexs...)
	return t
}

func (t TableConfig) WithHandler(handler Handler) TableConfig {
	if handler != nil {
		t._handler = handler // 此处需要兼容事务句柄设置，不可影响已有的handler设置,所以不能使用地址引用方式覆盖，而是返回一个新的TableConfig实例
	}
	return t
}

func (t TableConfig) GetHandler() (handler Handler) {
	if t._handler == nil {
		err := errors.New("database handler is nil, please use TableConfig.WithHandler to set handler")
		panic(err)
	}
	t.Init() //初始化数据表，比如启用事件订阅等
	return t._handler
}

func (t TableConfig) GetHandlerWithInitTable() (handler Handler) {
	handler = t.GetHandler()
	if shouldCrateTable(t.Name, Driver(handler.GetDialector())) {
		sql := fmt.Sprintf(`select 1 from %s;`, t.DBName.BaseNameWithQuotes())
		ctx := context.Background()

		var result int
		err := handler.Query(ctx, sql, &result)
		if err != nil { // 查询报错，则认为是表不存在，则创建表
			ddl, err := t.GenerateDDL()
			if err != nil {
				panic(err)
			}
			err = handler.Exec(ddl)
			if err != nil {
				err = errors.WithMessagef(err, "create table:%s failed", t.Name)
				panic(err)
			}
			fmt.Println(ddl)
		}
	}
	return handler
}
func (t TableConfig) GetDBNameByFieldNameMust(fieldName string) (dbName string) {
	col, err := t.Columns.GetByFieldNameAsError(fieldName)
	if err != nil {
		err := errors.Errorf("table(%s) ColumnConfig not found by fieldName:%s ", t.Name, fieldName) //增加table name 信息,便于排查问题
		panic(err)
	}
	return col.DbName
}
func (t TableConfig) GetDBNameByFieldName(fieldName string) (dbName string) {
	col, _ := t.Columns.GetByFieldName(fieldName)
	return col.DbName
}

// Deprecated: use modelMiddleware instead
func (t TableConfig) MergeTableLevelFields(ctx context.Context, scene Scene, fs ...*Field) Fields {
	fs1 := Fields(fs) //修改类型
	if t.tableLevelFieldsHook != nil {
		moreFields := t.tableLevelFieldsHook(ctx, scene)
		fs1 = fs1.Append(moreFields...)
	}
	return fs1
}

//Fields 返回所有字段，包括表级别字段,主要用于api 出入参生成文档场景

func (t TableConfig) Fields() (fs Fields) {
	return t.Columns.Fields()
}

func (t TableConfig) GetFieldNameByAlaisFeild(alaisField *Field) (fieldName string, err error) {
	fieldName, err = t.Columns.Fields().getNameByAliasName(alaisField.Name)
	if err != nil {
		err = errors.WithMessagef(err, "table:%s", t.Name)
		return "", err
	}
	return fieldName, nil
}

var Error_UniqueIndexAlreadyExist = errors.New("unique index already exist")

func (t TableConfig) CheckUniqueIndex(allFields ...*Field) (err error) {
	indexs := t.Indexs.GetUnique()
	for _, index := range indexs {
		uFs := index.Fields(t, allFields).AppendWhereValueFn(ValueFnForward) // 变成查询条件
		columnNames := index.GetColumnNames(t)
		if len(uFs) != len(columnNames) { // 如果唯一标识字段数量和筛选条件字段数量不一致，则忽略该唯一索引校验（如 update 时不涉及到指定唯一索引）
			continue
		}
		exists, err := NewExistsBuilder(t).WithHandler(t.GetHandler()).AppendFields(uFs...).Exists()
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

func (ts TableConfigs) Init() (err error) {
	for _, t := range ts {
		err = t.Init()
		if err != nil {
			return err
		}
	}
	return nil
}
func (ts TableConfigs) GetByName(name string) (t *TableConfig, exists bool) {
	if name == "" {
		return nil, false
	}
	t, exists = funcs.GetOne(ts, func(t TableConfig) bool { return t.Name == name })
	return t, exists
}

func (ts TableConfigs) Fields() (fs Fields) {
	for _, t := range ts {
		fs.Append(t.Columns.Fields()...)
	}
	return fs
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
		if t.Name == "" {
			t = table
			continue
		}
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
		if table.Indexs != nil {
			t.Indexs = t.Indexs.Merge(table, table.Indexs...)
		}
		if table._handler == nil {
			t._handler = table._handler
		}
		if table.Comment != "" {
			t.Comment = table.Comment
		}
		if table.tableLevelFieldsHook != nil {
			t.tableLevelFieldsHook = table.tableLevelFieldsHook
		}

		if len(table.modelMiddlewares) > 0 {
			t.modelMiddlewares = t.modelMiddlewares.append(table.modelMiddlewares...)
		}
		if table.shardedTableNameFn != nil {
			t.shardedTableNameFn = table.shardedTableNameFn
		}
		if table.comsumerMakers != nil {
			t.comsumerMakers = append(t.comsumerMakers, table.comsumerMakers...)
		}
		if table.Schema.Name != "" {
			t.Schema = table.Schema
		}
	}
	return t
}

type ColumnConfig struct {
	FieldName     string     // 业务标识 和Field.Name 保持一致，用户 column 和Field 互转
	DbName        string     `json:"dbName"` // 数据库字段名，和数据库字段保持一致
	Type          SchemaType `json:"type"`
	Unsigned      bool       `json:"unsigned"`
	AutoIncrement bool       `json:"autoIncrement"`
	Length        int        `json:"length"`
	Maximum       uint       `json:"maximum"` // 最大值，
	NotNull       bool       `json:"nullable"`
	Default       any        `json:"default"`
	Comment       string     `json:"comment"`
	Enums         Enums      `json:"enums"`
	Tags          Tags       `json:"tags"`
	ddlSort       int        //DDL 排序位置，用于DDL生成顺序(before xxx,after xxx xxx 代表字段，last 代表最后，first 代表最前)
	field         *Field
}

const (
	DDLSort_First = math.MaxInt
	DDLSort_Last  = 0
)

func (c ColumnConfig) WithDDLSort(sort int) ColumnConfig {
	c.ddlSort = sort
	return c
}

func (c ColumnConfig) WithType(dbColType SchemaType) ColumnConfig {
	c.Type = dbColType
	return c
}
func (c ColumnConfig) Identity() string {
	key := fmt.Sprintf("%s_%s", c.DbName, c.FieldName)
	return key
}
func (c ColumnConfig) WithLength(length int) ColumnConfig {
	c.Length = length
	return c
}
func (c ColumnConfig) WithNullable(nullable bool) ColumnConfig {
	c.NotNull = nullable
	return c
}
func (c ColumnConfig) WithUnSigned(unsigned bool) ColumnConfig {
	c.Unsigned = unsigned
	return c
}
func (c ColumnConfig) WithAutoIncrement(autoIncr bool) ColumnConfig {
	c.AutoIncrement = autoIncr
	return c
}
func (c ColumnConfig) WithDefault(defaultValue any) ColumnConfig {
	c.Default = defaultValue
	return c
}
func (c ColumnConfig) WithComment(comment string) ColumnConfig {
	c.Comment = comment
	return c
}
func (c ColumnConfig) WithTags(tags ...string) ColumnConfig {
	c.Tags = Tags(tags)
	return c
}

func (c ColumnConfig) WithField(f *Field) ColumnConfig {
	c.field = f
	return c
}

func (c ColumnConfig) WithEnums(enums ...Enum) ColumnConfig {
	c.Enums = Enums(enums)
	return c
}
func (c ColumnConfig) Copy() ColumnConfig {
	cp := c
	if cp.field != nil {
		cp.field = cp.field.Copy()
	}
	if c.Enums != nil {
		cp.Enums = cp.Enums.Copy()

	}
	return cp
}

func (c ColumnConfig) CopyFieldSchemaIfEmpty() ColumnConfig {
	if c.field == nil || c.field.Schema == nil {
		return c
	}
	field := c.field
	c.Tags.Append(field.tags...)
	if field.HastTag(Tag_autoIncrement) {
		c.AutoIncrement = true
	}
	if field.HastTag(Tag_unsigned) {
		c.Unsigned = true
	}
	fieldSchema := field.Schema
	if c.Type == "" {
		c.Type = fieldSchema.Type
	}
	if c.Length == 0 {
		c.Length = fieldSchema.MaxLength
	}
	if c.Maximum == 0 {
		c.Maximum = fieldSchema.Maximum
	}
	if fieldSchema.Minimum != nil && *fieldSchema.Minimum >= 0 {
		c.Unsigned = true // 最小值大于等于0，认为是无符号类型
	}

	if !c.NotNull {
		c.NotNull = fieldSchema.Required
	}
	if c.Default == nil {
		c.Default = fieldSchema.Default
	}
	if c.Comment == "" {
		c.Comment = fieldSchema.Comment
		if c.Comment == "" {
			c.Comment = fieldSchema.Title
		}
	}
	if len(c.Enums) == 0 {
		c.Enums = fieldSchema.Enums
	}
	return c

}

func (c ColumnConfig) GetField() *Field {
	if c.field != nil {
		cp := c.field.Copy()
		return cp
	}
	f := NewField(0).SetName(c.CamelName()).SetType(c.Type).Comment(c.Comment).AppendEnum(c.Enums...).SetDefault(c.Default)
	if c.Type.IsEqual(Schema_Type_string) {
		f.SetLength(c.Length)
	}
	//todo 更多细节设置,如根据默认值和Nullable设置是否容许为空等
	return f
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

	return newColumnConfig(dbFieldName, field.Name).WithField(field).CopyFieldSchemaIfEmpty()
}
func NewColumns(dbFieldName string, fs ...*Field) ColumnConfigs {
	cols := make([]ColumnConfig, 0)
	for _, f := range fs {
		cols = append(cols, newColumnConfig(dbFieldName, f.Name).WithField(f).CopyFieldSchemaIfEmpty())
	}
	return cols
}

// DbNameIsEmpty 字段DbName 为空，对数据表操作来说，是无效字段，但是业务层面封装的模型支持退化，比如keyvalue 模型正常2个字段，但是只保留key 也是常见模型，这是将value映射为""，即可实现key模型，于是诞生 了DbName 为空的数据，此函数协助过滤
func (c ColumnConfig) DbNameIsEmpty() bool {
	return c.DbName == ""
}

func (c ColumnConfig) CamelName() string {
	return funcs.CamelCase(c.DbName, false, false)
}

func (c ColumnConfig) MakeField(value any) *Field {

	// valueFnFn := func(_ any, f *Field, fs ...*Field) (any, error) {
	// 	return value, nil
	// }
	f := c.GetField().SetValue(value)
	//f.ValueFns.ResetSetValueFn(valueFnFn)
	return f
}

type ColumnConfigs []ColumnConfig

func (cs ColumnConfigs) sort() {
	slices.SortStableFunc(cs, func(a, b ColumnConfig) int {
		return b.ddlSort - a.ddlSort
	})
}

func (cs ColumnConfigs) InitDDLSort() ColumnConfigs {
	for i := range cs {
		cs[i].ddlSort = 1 * 100 // 列与列之间预留100个位置，便于后续插入列
	}
	return cs
}

// GetDDLSort 获取字段DDL排序位置，如果不存在返回0,false,使用前确保ColumnConfigs已经设置好DDL排序位置，否则结果不准确（可以使用ColumnConfigs.InitDDLSort先设置）
func (cs ColumnConfigs) GetDDLSort(dbName string) (sort int, exitst bool) {
	for _, c := range cs {
		if strings.EqualFold(c.DbName, dbName) {
			return c.ddlSort, true
		}
	}
	return 0, false
}

func (cs ColumnConfigs) Fields() (fs Fields) {
	for _, c := range cs {
		fs = append(fs, c.GetField())
	}
	return fs
}

func (cs ColumnConfigs) DBNames() (dbNames []string) {
	for _, c := range cs {
		dbNames = append(dbNames, c.DbName)
	}
	dbNames = memorytable.NewTable(dbNames...).Uniqueue(func(row string) (key string) { return row })
	return dbNames

}

func (cs *ColumnConfigs) AddColumns(cols ...ColumnConfig) {
	//2025-06-18 09:50 注释去重，将 数据库字段和业务字段改为1:N关系，理由：
	// 1. 业务上存在多个字段对应数据库一个字段，比如id,ids
	// 2. 改成1:N 关系后,提前封装的业务模型内数据表字段映射不会影响实际业务字段表映射的完整性，相当于站在各自的领域角度，操作同一个字段，能有效解耦提前封装的业务模块和实际扩展的模型
	// 3. 改成1:N 关系后,基本没有副作用，在通过Columns 生成ddl时可以根据dbName 去重即可
	if *cs == nil {
		*cs = make([]ColumnConfig, 0)
	}
	m := make(map[string]struct{})
	for _, c := range *cs {
		key := c.Identity()
		m[key] = struct{}{}
	}
	for _, c := range cols {
		key := c.Identity()
		if _, exists := m[key]; !exists {
			m[key] = struct{}{}
			*cs = append(*cs, c)
		}
	}
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

// CheckMissOutFieldName 检查缺失的字段名，如果有则返回错误。主要用于在封装模型时，检测必备字段是否存在
func (cs ColumnConfigs) CheckMissOutFieldName(fieldNames ...string) (err error) {
	for _, fieldName := range fieldNames {
		_, exists := cs.GetByFieldName(fieldName)
		if !exists {
			err = errors.Errorf("ColumnConfig not found by fieldName: " + string(fieldName))
			return err
		}
	}
	return nil
}

// deprecated 请使用TableConfig.WithAlaisTableConfig 代替 CheckMissFieldName package 封装模块时，用于检测模块内置的字段是否包含到提供的表配置中
func CheckMissFieldName(tableConfig TableConfig, fieldNames ...string) (err error) {
	err = tableConfig.Columns.CheckMissOutFieldName(fieldNames...)
	if err != nil {
		err = errors.WithMessagef(err, "table:%s;table.columnName:Field=1:N;use TableConfig.AddColumn(...) set it", tableConfig.Name)
		return err
	}
	return nil
}

func (cs ColumnConfigs) Merge(others ...ColumnConfig) ColumnConfigs {
	//cs = append(cs, others...)
	cs.AddColumns(others...) // 这里使用AddColumns，是为了支持同名覆盖
	return cs
}

// GetByFieldName  通过标识获取列配置信息，找不到则panic退出。主要用于生成字段时快速定位列配置信息。
func (cs ColumnConfigs) GetByFieldNameMust(fieldName string) (c ColumnConfig) {
	c, err := cs.GetByFieldNameAsError(fieldName)
	if err != nil {
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
func (cs ColumnConfigs) GetByFieldNameAsError(fieldName string) (c ColumnConfig, err error) {
	c, ok := cs.GetByFieldName(fieldName)
	if !ok {
		err = errors.Errorf("not found fieldName(%s) in Columns(dbNames:%s)", fieldName, strings.Join(cs.DBNames(), ","))
		return c, err
	}
	return c, nil
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
	if len(fieldNames) == 0 {
		return result
	}
	for _, c := range cs {
		if slices.Contains(fieldNames, c.FieldName) {
			result.AddColumns(c)
		}
	}
	return result
}

// EqualForFieldName 判断两个列配置是否包含相同的字段名，但不考虑字段别名等其他属性。可用于判断2个表模型是否一致
func (cs ColumnConfigs) EqualForFieldName(otherCS ColumnConfigs) bool {
	if len(cs) != len(otherCS) {
		return false
	}
	csFieldNames := cs.Fields().Names()
	for _, c := range otherCS {
		if !slices.Contains(csFieldNames, c.FieldName) {
			return false
		}
	}
	return true
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

func SafeGetSelectColumns(table TableConfig, in any) (columns []any) {
	all := []any{"*"}
	if in == nil {
		return all
	}
	fs, ok := TryGetFields(in)
	if ok {
		table.Columns.FilterByFieldName(fs.Names()...).DbNameWithAlias().AsAny()
	}

	return all
}

func TryGetFields(in any) (fs Fields, ok bool) {
	if fs, ok := in.(Fields); ok { // 优先尝试 Fields,提升性能
		return fs, true
	}

	if fsI, ok := in.(FieldsI); ok { // 优先尝试 FieldsI,提升性能
		return fsI.Fields(), true
	}

	//通用解决方案
	rt := reflect.TypeOf(in)
	// 归约到基础类型
	for rt.Kind() == reflect.Ptr || rt.Kind() == reflect.Slice {
		rt = rt.Elem()
	}

	// 优先尝试 User 零值
	var fieldsI FieldsI
	zeroRv := reflect.New(rt)
	fieldsI, ok = zeroRv.Interface().(FieldsI) // 尝试 *User 零值
	if !ok {
		fieldsI, ok = zeroRv.Elem().Interface().(FieldsI) // 尝试 User 零值
	}
	if ok {
		fs := fieldsI.Fields()
		return fs, true
	}
	return nil, false
}

type ModelWithFields struct {
	selectColumnsFields Fields
}

func (m *ModelWithFields) SetFields(fs ...*Field) {
	m.selectColumnsFields = fs
}
func (m *ModelWithFields) Fields() Fields {
	return m.selectColumnsFields
}

// GetRecordForUpdate 根据主键获取记录，主要用于生成冗余字段的值fs 必须包含主键字段值
func GetRecordForUpdate[Model any](fs Fields) (record *Model, err error) {
	if len(fs) == 0 {
		err = errors.Errorf("为更新查询的字段不能为空")
		return nil, err
	}
	first := fs.FirstMust()
	switch first.scene {
	case SCENE_SQL_INSERT:
		record = new(Model)
		return record, nil //新增场景，数据数据为空，直接返回空记录即可
	case SCENE_SQL_UPDATE:
		//继续后续代码
	default:
		err = errors.Errorf("字段场景必须是新增/更新场景")
		return nil, err
	}
	fs1 := fs.Copy()
	table := first.GetTable()

	limitField := NewIntField(2, "limit", "数量", 0).SetTag(Field_tag_pageSize) //最多查询2条记录
	fs1 = fs1.Add(limitField)
	records := make([]Model, 0)
	fs1.CleanSelectColumns() // 清理select字段，重新设置
	err = table.Repository().All(&records, fs1)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		err = errors.Errorf("没有查询到记录")

		return nil, err
	}
	if len(records) > 1 {
		err = errors.Errorf("查询到多条记录，无法确定要更新的记录")
		return nil, err
	}
	record = &records[0]
	return record, nil
}

type HookField struct {
	ObserveFields Fields
	DestField     *Field
	GetValueFn    func(scene Scene, updatingData DBDataMap, dbData DBDataMap, fs ...*Field) (val any, err error)
}

func MakeFieldHook(hookFields ...HookField) (hookFn HookFn) {
	return func(ctx context.Context, scena Scene) (hookedFields Fields) {
		for _, hookField := range hookFields {
			f := hookField.DestField.ResetValueFn(ValueFnSetFormat(ValueFnPreventDeadLoop(func(inputValue any, f *Field, fs ...*Field) (any, error) {
				fs1 := Fields(fs)
				if len(fs1) == 0 {
					return nil, nil
				}
				if !Fields(fs1).Contains(hookField.ObserveFields...) { // 本次操作不涉及关注的字段，则不操作冗余字段
					return nil, nil
				}

				scene := fs1.FirstMust().scene

				if !slices.Contains(SCENE_Commands, scene) { // 非命令场景，无需变更冗余数据, 这里的命令场景包含 删除、新增、修改场景，调用方可视情况屏蔽删除场景
					return nil, nil
				}

				updatingData, dbData, err := fs1.GetChangingData() //全量更新数据
				if err != nil {
					return nil, err
				}
				val, err := hookField.GetValueFn(scene, updatingData, dbData, fs1...)
				if err != nil {
					return nil, err
				}
				return val, nil
			})))
			hookedFields = hookedFields.Append(f)
		}
		return hookedFields
	}
}

// ValueFnPreventDeadLoop 防止冗余字段的valueFn陷入死循环,使用传入的fs 修改当前列的值, 忽略当前列，防止陷入死循环（这个问题已经遇见几次了，所以这里封装一个函数）
func ValueFnPreventDeadLoop(valueFnFn ValueFnFn) ValueFnFn {
	return func(inputValue any, f *Field, fs ...*Field) (any, error) {
		fs1 := Fields(fs).Filter(func(f1 Field) bool { // 需要忽略当前列，不然  fs1.GetChangingData() 会陷入死循环
			return f.Name != f1.Name
		})
		return valueFnFn(inputValue, f, fs1...)
	}
}
