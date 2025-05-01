package sqlbuilder

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

type CustomFnInsertParam func(insert *InsertParam)
type CustomFnBatchInsertParam func(insert *BatchInsertParam)

func (s RepositoryCommand) Insert(fields Fields, customFn CustomFnInsertParam) (err error) {
	builder := NewCompiler(s.getConfig(), fields...).Insert()
	if customFn != nil {
		customFn(builder)

	}
	err = builder.Exec()
	return err
}
func (s RepositoryCommand) BatchInsert(fieldsList []Fields, customFn CustomFnBatchInsertParam) (err error) {
	builder := NewCompiler(s.getConfig()).WithBatchFields(fieldsList...).InsertBatch()
	if customFn != nil {
		customFn(builder)

	}
	err = builder.Exec()
	return err
}

func (s RepositoryCommand) InsertWithLastId(fields Fields, customFn CustomFnInsertParam) (lastInsertId uint64, err error) {
	builder := NewCompiler(s.getConfig(), fields...).Insert()
	if customFn != nil {
		customFn(builder)
	}
	lastInsertId, _, err = builder.Insert()
	if err != nil {
		return 0, err
	}
	return lastInsertId, nil
}

type CustomFnUpdateParam func(insert *UpdateParam)

func (s RepositoryCommand) Update(fields Fields, customFn CustomFnUpdateParam) (err error) {
	builder := NewCompiler(s.getConfig(), fields...).Update()
	if customFn != nil {
		customFn(builder)
	}
	err = builder.Exec()
	return err
}

type CustomFnSetParam func(set *SetParam)

func (s RepositoryCommand) Set(fields Fields, custom CustomFnSetParam) (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	builder := NewCompiler(s.getConfig(), fields...).Set()
	if custom != nil {
		custom(builder)

	}
	isInsert, lastInsertId, rowsAffected, err = builder.Set()
	return isInsert, lastInsertId, rowsAffected, err
}

type CustomFnDeleteParam func(delete *DeleteParam)

func (s RepositoryCommand) Delete(fields Fields, customFn CustomFnDeleteParam) (err error) {
	builder := NewCompiler(s.getConfig(), fields...).Delete()
	if customFn != nil {
		customFn(builder)
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

type CustomFnFirstParam func(first *FirstParam)

func (s RepositoryQuery[Model]) First(fields Fields, customFn CustomFnFirstParam) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), fields...).First()
	if customFn != nil {
		customFn(builder)
	}
	exists, err = builder.First(&model)
	return model, exists, err
}

type CustomFnPaginationParam func(pagination *PaginationParam)

func (s RepositoryQuery[Model]) Pagination(fields Fields, customFn CustomFnPaginationParam) (models []Model, total int64, err error) {
	builder := NewCompiler(s.getConfig(), fields...).Pagination()
	if customFn != nil {
		customFn(builder)

	}
	models = make([]Model, 0)
	total, err = builder.Pagination(&models)
	return models, total, err
}

type CustomFnListParam func(listParam *ListParam)

func (s RepositoryQuery[Model]) All(fields Fields, customFn CustomFnListParam) (models []Model, err error) {
	builder := NewCompiler(s.getConfig(), fields...).List()
	if customFn != nil {
		customFn(builder)

	}
	models = make([]Model, 0)
	err = builder.List(&models)
	return models, err
}
func (s RepositoryQuery[Model]) GetByIdentityMust(fields Fields, customFn CustomFnFirstParam) (model Model, err error) {
	builder := NewCompiler(s.getConfig(), fields...).First()
	if customFn != nil {
		customFn(builder)
	}

	err = builder.FirstMustExists(&model)
	return model, err
}

func (s RepositoryQuery[Model]) GetByIdentity(fields Fields, customFn CustomFnFirstParam) (model Model, exists bool, err error) {
	builder := NewCompiler(s.getConfig(), fields...).First()
	if customFn != nil {
		customFn(builder)

	}
	exists, err = builder.First(&model)
	return model, exists, err
}

func (s RepositoryQuery[Model]) GetByIdentities(fields Fields, customFn CustomFnListParam) (models []Model, err error) {
	builder := NewCompiler(s.getConfig(), fields...).List()
	if customFn != nil {
		customFn(builder)

	}
	err = builder.List(models)
	return models, err
}
