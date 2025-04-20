package sqlbuilder

import "github.com/suifengpiao14/memorytable"

type RepositoryService struct {
	tableConfig TableConfig
	handler     Handler
}

func (s RepositoryService) getConfig() CompilerConfig {
	cfg := CompilerConfig{}.WithHandlerIgnore(s.handler).WithTableIgnore(s.tableConfig)
	return cfg
}

func (s RepositoryService) Insert(fields FieldsI) (err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).Insert()
	err = builder.Exec()
	return err
}
func (s RepositoryService) InsertWithLastId(fields FieldsI) (lastInsertId uint64, err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).Insert()
	lastInsertId, _, err = builder.Insert()
	if err != nil {
		return 0, err
	}
	return lastInsertId, nil
}

func (s RepositoryService) Update(fields FieldsI) (err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).Update()
	err = builder.Exec()
	return err
}

func (s RepositoryService) Delete(fields FieldsI) (err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).Delete()
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

func (s RepositoryQueryService[Model]) First(fields FieldsI) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).First()
	exists, err = builder.First(&model)
	return model, exists, err
}
func (s RepositoryQueryService[Model]) Pagination(dst any, fields FieldsI) (modelTable memorytable.Table[Model], total int64, err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).Pagination()
	modelTable = make([]Model, 0)
	total, err = builder.Pagination(&modelTable)
	return modelTable, total, err
}

func (s RepositoryQueryService[Model]) All(fields FieldsI) (modelTable memorytable.Table[Model], err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).List()
	modelTable = make([]Model, 0)
	err = builder.List(&modelTable)
	return modelTable, err
}
func (s RepositoryQueryService[Model]) GetByIdentityMust(fields FieldsI) (model Model, err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).First()
	err = builder.FirstMustExists(&model)
	return model, err
}

func (s RepositoryQueryService[Model]) GetByIdentity(fields FieldsI) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).First()
	exists, err = builder.First(&model)
	return model, exists, err
}
func (s RepositoryQueryService[Model]) GetByIdentities(fields FieldsI) (modelTable memorytable.Table[Model], err error) {
	builder := NewCompiler(s.getConfig(), fields.Fields()...).List()
	err = builder.List(modelTable)
	return modelTable, err
}
