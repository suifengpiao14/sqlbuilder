package sqlbuilder

import "context"

type SelectBuilderFnsI interface {
	SelectBuilderFn() (selectBuilder SelectBuilderFns)
}

type RepositoryCommand struct {
	tableConfig      TableConfig
	modelMiddlewares ModelMiddlewares
}

func (s RepositoryCommand) WithModelMiddleware(modelMiddlewares ...ModelMiddleware) RepositoryCommand {
	s.modelMiddlewares = modelMiddlewares
	return s
}

func NewRepositoryCommand(tableConfig TableConfig) RepositoryCommand {
	return RepositoryCommand{
		tableConfig: tableConfig,
	}
}

// Deprecated  use GetTableConfig.GetHander() instead.
func (s RepositoryCommand) GetHandler() Handler {
	return s.tableConfig.GetHandlerWithInitTable()
}

func (s RepositoryCommand) GetTableConfig() TableConfig {
	return s.tableConfig
}

func (s RepositoryCommand) Insert(ctx context.Context, fields Fields, customFns ...CustomFnInsertParam) (err error) {
	builder := NewInsertBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	err = builder.Exec()
	return err
}
func (s RepositoryCommand) BatchInsert(ctx context.Context, fieldsList []Fields, customFns ...CustomFnBatchInsertParam) (err error) {
	builder := NewBatchInsertBuilder(s.tableConfig).WithContext(ctx).AppendFields(fieldsList...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	err = builder.Exec()
	return err
}

func (s RepositoryCommand) InsertWithLastId(ctx context.Context, fields Fields, customFns ...CustomFnInsertParam) (lastInsertId uint64, rowsAffected int64, err error) {
	builder := NewInsertBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	lastInsertId, rowsAffected, err = builder.Insert()
	if err != nil {
		return 0, 0, err
	}
	return lastInsertId, rowsAffected, nil
}

func (s RepositoryCommand) Update(ctx context.Context, fields Fields, customFns ...CustomFnUpdateParam) (err error) {
	builder := NewUpdateBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	err = builder.Exec()
	return err
}
func (s RepositoryCommand) UpdateWithRowsAffected(ctx context.Context, fields Fields, customFns ...CustomFnUpdateParam) (rowsAffected int64, err error) {
	builder := NewUpdateBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	rowsAffected, err = builder.Update()
	return rowsAffected, err
}

func (s RepositoryCommand) Set(ctx context.Context, fields Fields, customFns ...CustomFnSetParam) (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	builder := NewSetBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	isInsert, lastInsertId, rowsAffected, err = builder.Set()
	return isInsert, lastInsertId, rowsAffected, err
}

func (s RepositoryCommand) Delete(ctx context.Context, fields Fields, customFns ...CustomFnDeleteParam) (err error) {
	builder := NewDeleteBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	err = builder.Exec()
	return err
}

type RepositoryQuery struct {
	tableConfig      TableConfig
	modelMiddlewares ModelMiddlewares
}

func NewRepositoryQuery(tableConfig TableConfig) RepositoryQuery {
	return RepositoryQuery{
		tableConfig: tableConfig,
	}
}

func (s RepositoryQuery) GetTableConfig() TableConfig {
	return s.tableConfig
}

// Deprecated  use GetTableConfig.GetHander() instead.
func (s RepositoryQuery) GetHandler() Handler {
	return s.tableConfig.GetHandlerWithInitTable()
}
func (s RepositoryQuery) WithModelMiddleware(modelMiddlewares ...ModelMiddleware) RepositoryQuery {
	s.modelMiddlewares = modelMiddlewares
	return s
}
func (s RepositoryQuery) First(ctx context.Context, dst any, fields Fields, customFns ...CustomFnFirstParam) (exists bool, err error) {
	builder := NewFirstBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	exists, err = builder.First(dst)
	return exists, err
}
func (s RepositoryQuery) FirstMustExists(ctx context.Context, dst any, fields Fields, customFns ...CustomFnFirstParam) (err error) {
	builder := NewFirstBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	err = builder.FirstMustExists(dst)
	return err
}

func (s RepositoryQuery) Pagination(ctx context.Context, dst any, fields Fields, customFns ...CustomFnPaginationParam) (total int64, err error) {
	builder := NewPaginationBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	total, err = builder.Pagination(dst)
	return total, err
}

func (s RepositoryQuery) All(ctx context.Context, dst any, fields Fields, customFns ...CustomFnListParam) (err error) {
	builder := NewListBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	err = builder.List(dst)
	return err
}

func (s RepositoryQuery) Exists(ctx context.Context, fields Fields, customFns ...CustomFnExistsParam) (exists bool, err error) {
	builder := NewExistsBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	exists, err = builder.Exists()
	return exists, err
}

func (s RepositoryQuery) Count(ctx context.Context, fields Fields, customFns ...CustomFnTotalParam) (total int64, err error) {
	builder := NewTotalBuilder(s.tableConfig).WithContext(ctx).AppendFields(fields...).ApplyCustomFn(customFns...).WithModelMiddleware(s.modelMiddlewares...)
	total, err = builder.Count()
	return total, err
}

type Repository struct {
	tableConfig TableConfig
	RepositoryCommand
	RepositoryQuery
}

func NewRepository(tableConfig TableConfig) Repository {
	return Repository{
		tableConfig:       tableConfig,
		RepositoryCommand: NewRepositoryCommand(tableConfig),
		RepositoryQuery:   NewRepositoryQuery(tableConfig),
	}
}

func (r Repository) GetTable() TableConfig {
	return r.tableConfig
}

func (s Repository) WithModelMiddleware(modelMiddlewares ...ModelMiddleware) Repository {
	s.RepositoryCommand = s.RepositoryCommand.WithModelMiddleware(modelMiddlewares...)
	s.RepositoryQuery = s.RepositoryQuery.WithModelMiddleware(modelMiddlewares...)
	return s
}

func (r Repository) Transaction(fc func(txRepository Repository) (err error)) (err error) {

	err = r.TransactionForMutiTable(func(tx Handler) error {
		tableConfig := r.tableConfig.WithHandler(tx)
		txRepo := Repository{
			tableConfig:       tableConfig,
			RepositoryCommand: NewRepositoryCommand(tableConfig),
			RepositoryQuery:   NewRepositoryQuery(tableConfig),
		}
		err = fc(txRepo)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (r Repository) TransactionForMutiTable(fc func(tx Handler) (err error)) (err error) {
	err = r.GetTable().GetHandlerWithInitTable().Transaction(fc)
	if err != nil {
		return err
	}
	return nil
}

func (r Repository) WithTxHandler(txHandler Handler) Repository {
	tableConfig := r.GetTable().WithHandler(txHandler)
	return Repository{
		tableConfig:       tableConfig,
		RepositoryCommand: NewRepositoryCommand(tableConfig),
		RepositoryQuery:   NewRepositoryQuery(tableConfig),
	}
}
