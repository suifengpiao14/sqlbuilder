package sqlbuilder

import "github.com/suifengpiao14/memorytable"

type RepositoryI interface {
	GetTable() TableConfig
	GetFields() Fields
}

type RepositoryCommandService struct {
	tableConfig TableConfig
	handler     Handler
}

func (s RepositoryCommandService) getConfig() CompilerConfig {
	cfg := CompilerConfig{}.WithHandlerIgnore(s.handler).WithTableIgnore(s.tableConfig)
	return cfg
}

func (s RepositoryCommandService) Insert(fieldsFn func() Fields) (err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).Insert()
	err = builder.Exec()
	return err
}
func (s RepositoryCommandService) InsertWithLastId(fieldsFn func() Fields) (lastInsertId uint64, err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).Insert()
	lastInsertId, _, err = builder.Insert()
	if err != nil {
		return 0, err
	}
	return lastInsertId, nil
}

func (s RepositoryCommandService) Update(fieldsFn func() Fields) (err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).Update()
	err = builder.Exec()
	return err
}

func (s RepositoryCommandService) Delete(fieldsFn func() Fields) (err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).Delete()
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

func (s RepositoryQueryService[Model]) First(fieldsFn func() Fields) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).First()
	exists, err = builder.First(&model)
	return model, exists, err
}
func (s RepositoryQueryService[Model]) Pagination(dst any, fieldsFn func() Fields) (modelTable memorytable.Table[Model], total int64, err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).Pagination()
	modelTable = make([]Model, 0)
	total, err = builder.Pagination(&modelTable)
	return modelTable, total, err
}

func (s RepositoryQueryService[Model]) All(fieldsFn func() Fields) (modelTable memorytable.Table[Model], err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).List()
	modelTable = make([]Model, 0)
	err = builder.List(&modelTable)
	return modelTable, err
}
func (s RepositoryQueryService[Model]) GetByIdentityMust(fieldsFn func() Fields) (model Model, err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).First()
	err = builder.FirstMustExists(&model)
	return model, err
}

func (s RepositoryQueryService[Model]) GetByIdentity(fieldsFn func() Fields) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).First()
	exists, err = builder.First(&model)
	return model, exists, err
}
func (s RepositoryQueryService[Model]) GetByIdentities(fieldsFn func() Fields) (modelTable memorytable.Table[Model], err error) {
	builder := NewCompiler(s.getConfig(), fieldsFn()...).List()
	err = builder.List(modelTable)
	return modelTable, err
}
