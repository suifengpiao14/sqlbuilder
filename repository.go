package sqlbuilder

import "github.com/suifengpiao14/memorytable"

type FieldsI interface {
	Fields() Fields
}

type FieldsFn func() (fields Fields)

func (fn FieldsFn) Fields() Fields {
	return fn()
}

//MakeFieldsI 生成一个FieldsI接口的实现，用于传递字段信息。
func MakeFieldsI(fields ...*Field) FieldsI {
	var fn FieldsFn = func() Fields {
		return fields
	}

	return fn
}

type SelectBuilderFnsI interface {
	SelectBuilderFn() (selectBuilder SelectBuilderFns)
}

type RepositoryCommandService struct {
	tableConfig TableConfig
	handler     Handler
}

func NewRepositoryCommandService(tableConfig TableConfig, handler Handler) RepositoryCommandService {
	return RepositoryCommandService{
		tableConfig: tableConfig,
		handler:     handler,
	}
}

func (s RepositoryCommandService) getConfig() CompilerConfig {
	cfg := CompilerConfig{}.WithHandlerIgnore(s.handler).WithTableIgnore(s.tableConfig)
	return cfg
}

func (s RepositoryCommandService) Insert(fieldsI FieldsI, fieldsIs ...FieldsI) (err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).Insert()
	err = builder.Exec()
	return err
}
func (s RepositoryCommandService) InsertWithLastId(fieldsI FieldsI, fieldsIs ...FieldsI) (lastInsertId uint64, err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).Insert()
	lastInsertId, _, err = builder.Insert()
	if err != nil {
		return 0, err
	}
	return lastInsertId, nil
}

func (s RepositoryCommandService) Update(fieldsI FieldsI, fieldsIs ...FieldsI) (err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).Update()
	err = builder.Exec()
	return err
}

func (s RepositoryCommandService) Set(fieldsI FieldsI, fieldsIs ...FieldsI) (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).Set()
	isInsert, lastInsertId, rowsAffected, err = builder.Set()
	return isInsert, lastInsertId, rowsAffected, err
}

func (s RepositoryCommandService) Delete(fieldsI FieldsI, fieldsIs ...FieldsI) (err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).Delete()
	err = builder.Exec()
	return err
}

type RepositoryQueryService[Model any] struct {
	tableConfig TableConfig
	handler     Handler
}

func (s RepositoryQueryService[Model]) getConfig() CompilerConfig {
	cfg := CompilerConfig{}.WithHandlerIgnore(s.handler).WithTableIgnore(s.tableConfig)
	return cfg
}

func (s RepositoryQueryService[Model]) First(fieldsI FieldsI, fieldsIs ...FieldsI) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).First()
	selectBuilderFns := mergeSelectBuilderFns(fieldsI, fieldsIs...)
	builder = builder.WithBuilderFns(selectBuilderFns...)
	exists, err = builder.First(&model)
	return model, exists, err
}
func (s RepositoryQueryService[Model]) Pagination(fieldsI FieldsI, fieldsIs ...FieldsI) (modelTable memorytable.Table[Model], total int64, err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).Pagination()
	selectBuilderFns := mergeSelectBuilderFns(fieldsI, fieldsIs...)
	builder = builder.WithBuilderFns(selectBuilderFns...)
	modelTable = make([]Model, 0)
	total, err = builder.Pagination(&modelTable)
	return modelTable, total, err
}

func (s RepositoryQueryService[Model]) All(fieldsI FieldsI, fieldsIs ...FieldsI) (modelTable memorytable.Table[Model], err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).List()
	selectBuilderFns := mergeSelectBuilderFns(fieldsI, fieldsIs...)
	builder = builder.WithBuilderFns(selectBuilderFns...)
	modelTable = make([]Model, 0)
	err = builder.List(&modelTable)
	return modelTable, err
}
func (s RepositoryQueryService[Model]) GetByIdentityMust(fieldsI FieldsI, fieldsIs ...FieldsI) (model Model, err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).First()
	selectBuilderFns := mergeSelectBuilderFns(fieldsI, fieldsIs...)
	builder = builder.WithBuilderFns(selectBuilderFns...)
	err = builder.FirstMustExists(&model)
	return model, err
}

func (s RepositoryQueryService[Model]) GetByIdentity(fieldsI FieldsI, fieldsIs ...FieldsI) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).First()
	selectBuilderFns := mergeSelectBuilderFns(fieldsI, fieldsIs...)
	builder = builder.WithBuilderFns(selectBuilderFns...)
	exists, err = builder.First(&model)
	return model, exists, err
}
func (s RepositoryQueryService[Model]) GetByIdentities(fieldsI FieldsI, fieldsIs ...FieldsI) (modelTable memorytable.Table[Model], err error) {
	builder := NewCompiler(s.getConfig(), mergeFields(fieldsI, fieldsIs...)...).List()
	selectBuilderFns := mergeSelectBuilderFns(fieldsI, fieldsIs...)
	builder = builder.WithBuilderFns(selectBuilderFns...)
	err = builder.List(modelTable)
	return modelTable, err
}

func mergeFields(fieldsI FieldsI, fieldsIs ...FieldsI) (fields Fields) {
	fields = append(fields, fieldsI.Fields()...)
	for _, fieldIs := range fieldsIs {
		fields = append(fields, fieldIs.Fields()...)
	}
	return fields
}

func mergeSelectBuilderFns[T any](fieldsI T, fieldsIs ...T) (selectBuilderFns SelectBuilderFns) {
	all := make([]T, 0)
	all = append(all, fieldsI)
	all = append(all, fieldsIs...)

	for _, an := range all {
		SelectBuilderFns, ok := any(an).(SelectBuilderFnsI)
		if ok {
			selectBuilderFns = append(selectBuilderFns, SelectBuilderFns.SelectBuilderFn()...)
		}
	}
	return selectBuilderFns
}
