package sqlbuilder

type SelectBuilderFnsI interface {
	SelectBuilderFn() (selectBuilder SelectBuilderFns)
}

type RepositoryCommand struct {
	tableConfig TableConfig
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

func (s RepositoryCommand) Insert(fields Fields, customFns ...CustomFnInsertParam) (err error) {
	builder := NewInsertBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	err = builder.Exec()
	return err
}
func (s RepositoryCommand) BatchInsert(fieldsList []Fields, customFns ...CustomFnBatchInsertParam) (err error) {
	builder := NewBatchInsertBuilder(s.tableConfig).AppendFields(fieldsList...).ApplyCustomFn(customFns...)
	err = builder.Exec()
	return err
}

func (s RepositoryCommand) InsertWithLastId(fields Fields, customFns ...CustomFnInsertParam) (lastInsertId uint64, rowsAffected int64, err error) {
	builder := NewInsertBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	lastInsertId, rowsAffected, err = builder.Insert()
	if err != nil {
		return 0, 0, err
	}
	return lastInsertId, rowsAffected, nil
}

func (s RepositoryCommand) Update(fields Fields, customFns ...CustomFnUpdateParam) (err error) {
	builder := NewUpdateBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	err = builder.Exec()
	return err
}
func (s RepositoryCommand) UpdateWithRowsAffected(fields Fields, customFns ...CustomFnUpdateParam) (rowsAffected int64, err error) {
	builder := NewUpdateBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	rowsAffected, err = builder.Update()
	return rowsAffected, err
}

func (s RepositoryCommand) Set(fields Fields, customFns ...CustomFnSetParam) (isInsert bool, lastInsertId uint64, rowsAffected int64, err error) {
	builder := NewSetBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	isInsert, lastInsertId, rowsAffected, err = builder.Set()
	return isInsert, lastInsertId, rowsAffected, err
}

func (s RepositoryCommand) Delete(fields Fields, customFns ...CustomFnDeleteParam) (err error) {
	builder := NewDeleteBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	err = builder.Exec()
	return err
}

// type ModelI any
// type RepositoryQuery[Model ModelI] struct {
// 	tableConfig TableConfig
// }

// func NewRepositoryQuery[Model ModelI](tableConfig TableConfig) RepositoryQuery[Model] {
// 	return RepositoryQuery[Model]{
// 		tableConfig: tableConfig,
// 	}
// }

// func (s RepositoryQuery[Model]) GetTableConfig() TableConfig {
// 	return s.tableConfig
// }

// // Deprecated  use GetTableConfig.GetHander() instead.
// func (s RepositoryQuery[Model]) GetHandler() Handler {
// 	return s.tableConfig.GetHandlerWithInitTable()
// }

// func (s RepositoryQuery[Model]) First(fields Fields, customFns ...CustomFnFirstParam) (model Model, exists bool, err error) {
// 	builder := NewFirstBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	exists, err = builder.First(&model)
// 	return model, exists, err
// }
// func (s RepositoryQuery[Model]) FirstMustExists(fields Fields, customFns ...CustomFnFirstParam) (model Model, err error) {
// 	builder := NewFirstBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	err = builder.FirstMustExists(&model)
// 	return model, err
// }

// func (s RepositoryQuery[Model]) Pagination(fields Fields, customFns ...CustomFnPaginationParam) (models []Model, total int64, err error) {
// 	builder := NewPaginationBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	models = make([]Model, 0)
// 	total, err = builder.Pagination(&models)
// 	return models, total, err
// }

// func (s RepositoryQuery[Model]) All(fields Fields, customFns ...CustomFnListParam) (models []Model, err error) {
// 	builder := NewListBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	models = make([]Model, 0)
// 	err = builder.List(&models)
// 	return models, err
// }
// func (s RepositoryQuery[Model]) GetByIdentityMust(fields Fields, customFns ...CustomFnFirstParam) (model Model, err error) {
// 	builder := NewFirstBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	builder.ApplyCustomFn(customFns...)

// 	err = builder.FirstMustExists(&model)
// 	return model, err
// }

// func (s RepositoryQuery[Model]) GetByIdentity(fields Fields, customFns ...CustomFnFirstParam) (model Model, exists bool, err error) {
// 	builder := NewFirstBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	exists, err = builder.First(&model)
// 	return model, exists, err
// }

// func (s RepositoryQuery[Model]) GetByIdentities(fields Fields, customFns ...CustomFnListParam) (models []Model, err error) {
// 	builder := NewListBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	err = builder.List(models)
// 	return models, err
// }

// func (s RepositoryQuery[Model]) Exists(fields Fields, customFns ...CustomFnExistsParam) (exists bool, err error) {
// 	builder := NewExistsBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	exists, err = builder.Exists()
// 	return exists, err
// }

// func (s RepositoryQuery[Model]) Count(fields Fields, customFns ...CustomFnTotalParam) (total int64, err error) {
// 	builder := NewTotalBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
// 	total, err = builder.Count()
// 	return total, err
// }

type RepositoryQuery struct {
	tableConfig TableConfig
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

func (s RepositoryQuery) First(dst any, fields Fields, customFns ...CustomFnFirstParam) (exists bool, err error) {
	builder := NewFirstBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	exists, err = builder.First(dst)
	return exists, err
}
func (s RepositoryQuery) FirstMustExists(dst any, fields Fields, customFns ...CustomFnFirstParam) (err error) {
	builder := NewFirstBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	err = builder.FirstMustExists(dst)
	return err
}

func (s RepositoryQuery) Pagination(dst any, fields Fields, customFns ...CustomFnPaginationParam) (total int64, err error) {
	builder := NewPaginationBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	total, err = builder.Pagination(dst)
	return total, err
}

func (s RepositoryQuery) All(dst any, fields Fields, customFns ...CustomFnListParam) (err error) {
	builder := NewListBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	err = builder.List(dst)
	return err
}

func (s RepositoryQuery) Exists(fields Fields, customFns ...CustomFnExistsParam) (exists bool, err error) {
	builder := NewExistsBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
	exists, err = builder.Exists()
	return exists, err
}

func (s RepositoryQuery) Count(fields Fields, customFns ...CustomFnTotalParam) (total int64, err error) {
	builder := NewTotalBuilder(s.tableConfig).AppendFields(fields...).ApplyCustomFn(customFns...)
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
