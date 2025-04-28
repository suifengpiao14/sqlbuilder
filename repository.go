package sqlbuilder

import (
	"github.com/suifengpiao14/memorytable"
)

type SelectBuilderFnsI interface {
	SelectBuilderFn() (selectBuilder SelectBuilderFns)
}

type RepositoryCommand struct {
	tableConfig TableConfig
	handler     Handler
}

func NewRepositoryCommand(tableConfig TableConfig) RepositoryCommand {
	return RepositoryCommand{
		tableConfig: tableConfig,
		handler:     tableConfig.handler,
	}
}

func (s RepositoryCommand) getConfig() CompilerConfig {
	cfg := CompilerConfig{}.WithHandlerIgnore(s.handler).WithTableIgnore(s.tableConfig)
	return cfg
}

func (s RepositoryCommand) Insert(fields Fields, customFn func(insertParam *InsertParam) (err error)) (err error) {
	builder := NewCompiler(s.getConfig(), fields...).Insert()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return err
		}
	}
	err = builder.Exec()
	return err
}
func (s RepositoryCommand) InsertWithLastId(fields Fields, customFn func(insertParam *InsertParam) (err error)) (lastInsertId uint64, err error) {
	builder := NewCompiler(s.getConfig(), fields...).Insert()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return 0, err
		}
	}
	lastInsertId, _, err = builder.Insert()
	if err != nil {
		return 0, err
	}
	return lastInsertId, nil
}

func (s RepositoryCommand) Update(fields Fields, customFn func(updateParam *UpdateParam) (err error)) (err error) {
	builder := NewCompiler(s.getConfig(), fields...).Update()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return err
		}
	}
	err = builder.Exec()
	return err
}

func (s RepositoryCommand) Set(fields Fields, custom func(setParam *SetParam) (err error)) (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	builder := NewCompiler(s.getConfig(), fields...).Set()
	if custom != nil {
		err = custom(builder)
		if err != nil {
			return false, 0, 0, err
		}
	}
	isInsert, lastInsertId, rowsAffected, err = builder.Set()
	return isInsert, lastInsertId, rowsAffected, err
}

func (s RepositoryCommand) Delete(fields Fields, customFn func(delete *DeleteParam) (err error)) (err error) {
	builder := NewCompiler(s.getConfig(), fields...).Delete()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return err
		}
	}
	err = builder.Exec()
	return err
}

type RepositoryQuery[Model any] struct {
	tableConfig TableConfig
	handler     Handler
}

func NewRepositoryQuery[Model any](tableConfig TableConfig) RepositoryQuery[Model] {
	return RepositoryQuery[Model]{
		tableConfig: tableConfig,
		handler:     tableConfig.handler,
	}
}

func (s RepositoryQuery[Model]) getConfig() CompilerConfig {
	cfg := CompilerConfig{}.WithHandlerIgnore(s.handler).WithTableIgnore(s.tableConfig)
	return cfg
}

func (s RepositoryQuery[Model]) First(fields Fields, customFn func(first *FirstParam) (err error)) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), fields...).First()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return model, false, err
		}
	}
	exists, err = builder.First(&model)
	return model, exists, err
}
func (s RepositoryQuery[Model]) Pagination(fields Fields, customFn func(pagination *PaginationParam) (err error)) (modelTable memorytable.Table[Model], total int64, err error) {
	builder := NewCompiler(s.getConfig(), fields...).Pagination()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return nil, 0, err
		}
	}
	modelTable = make([]Model, 0)
	total, err = builder.Pagination(&modelTable)
	return modelTable, total, err
}

func (s RepositoryQuery[Model]) All(fields Fields, customFn func(listParam *ListParam) (err error)) (modelTable memorytable.Table[Model], err error) {
	builder := NewCompiler(s.getConfig(), fields...).List()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return nil, err
		}
	}
	modelTable = make([]Model, 0)
	err = builder.List(&modelTable)
	return modelTable, err
}
func (s RepositoryQuery[Model]) GetByIdentityMust(fields Fields, customFn func(firstParam *FirstParam) (err error)) (model Model, err error) {
	builder := NewCompiler(s.getConfig(), fields...).First()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return model, err
		}
	}

	err = builder.FirstMustExists(&model)
	return model, err
}

func (s RepositoryQuery[Model]) GetByIdentity(fields Fields, customFn func(firstParam *FirstParam) (err error)) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), fields...).First()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return model, false, err
		}
	}
	exists, err = builder.First(&model)
	return model, exists, err
}
func (s RepositoryQuery[Model]) GetByIdentities(fields Fields, customFn func(listParam *ListParam) (err error)) (modelTable memorytable.Table[Model], err error) {
	builder := NewCompiler(s.getConfig(), fields...).List()
	if customFn != nil {
		err = customFn(builder)
		if err != nil {
			return modelTable, err
		}
	}
	err = builder.List(modelTable)
	return modelTable, err
}
