package sqlbuilder

import (
	"reflect"

	"github.com/doug-martin/goqu/v9"
	"github.com/pkg/errors"
)

// PaginationParamForShardedTable 从分表/水平分表中获取数据，分表/水平分表指 表结构相同，仅表名不同的多张表
type ShardedTablePaginationParam struct {
	PaginationParam
	withOutTotal bool // 默认统计总数，设置为true时不统计总数
}

func (p *ShardedTablePaginationParam) WithOutTotal(withOutTotal bool) *ShardedTablePaginationParam {
	p.withOutTotal = withOutTotal
	return p
}

func NewShardedTablePaginationBuilder(paginationParam PaginationParam) *ShardedTablePaginationParam {
	p := &ShardedTablePaginationParam{}
	p.PaginationParam = paginationParam
	return p
}

// shardedTableSingleTablePagination 单表操作
type shardedTableSingleTablePagination struct {
	table TableConfig // 表名
	//hitCount int64       // 符合条件的记录数
	p ShardedTablePaginationParam
}

func (shardedT shardedTableSingleTablePagination) Count() (count int64, err error) {
	// 执行计数查询
	handler := shardedT.p.getHandler()
	totalSql, err := shardedT.TotalSQL()
	if err != nil {
		return 0, err
	}
	count, err = handler.Count(totalSql)
	if err != nil {
		return 0, err
	}
	//shardedT.hitCount = count
	return count, nil
}

func (shardedT shardedTableSingleTablePagination) List(result any, offset, limit int) (err error) {
	// 查询记录
	handler := shardedT.p.getHandler()
	listSql, err := shardedT.ListSQL(offset, limit)
	if err != nil {
		return err
	}
	err = handler.Query(shardedT.p.context, listSql, result)
	if err != nil {
		return err
	}
	return nil
}

func (shardedT shardedTableSingleTablePagination) TotalSQL() (totalSql string, err error) {
	table := shardedT.table
	p := shardedT.p
	totalSql, err = NewTotalBuilder(table).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).WithBuilderFns(p.builderFns...).ToSQL()
	if err != nil {
		return "", err
	}
	return totalSql, nil
}

func (shardedT shardedTableSingleTablePagination) ListSQL(offset, limit int) (listSQL string, err error) {
	offset, limit = max(offset, 0), max(limit, 0)
	listBuilder := NewListBuilder(shardedT.table).WithCustomFieldsFn(shardedT.p.customFieldsFns...).AppendFields(shardedT.p._Fields...).WithBuilderFns(shardedT.p.builderFns...)
	listBuilder = listBuilder.WithBuilderFns(func(ds *goqu.SelectDataset) *goqu.SelectDataset {
		ds = ds.Offset(uint(offset)).Limit(uint(limit)) //根据实际情况 重置limit和offset

		return ds
	})
	listSql, err := listBuilder.ToSQL()
	if err != nil {
		return "", err
	}
	return listSql, nil
}

type ShardedTablePaginations []shardedTableSingleTablePagination

var ErrPaginationSizeRequired = errors.New("pagination size required")

func (p ShardedTablePaginationParam) Pagination(result any) (totalCount int64, err error) {
	rv := reflect.Indirect(reflect.ValueOf(result))
	rt := rv.Type()
	if rt.Kind() != reflect.Slice {
		err = errors.Errorf("result must be slice,got:%s", rt)
		return 0, err
	}
	if !rv.CanSet() {
		err = errors.Errorf("result must be CanSet,got:%s", rt)
		return 0, err
	}

	tableConfig := p.GetTable()
	shardedTablePaginations := ShardedTablePaginations{}
	tableNames := tableConfig.getShardedTableNames()
	if len(tableNames) == 0 { // 非分表场景，直接执行查询
		tableNames = []string{tableConfig.DBName.Name}
	}

	//生成统计总数sql
	for _, tableName := range tableNames {
		table := tableConfig.WithTableName(tableName)
		shardedTablePagination := shardedTableSingleTablePagination{
			table: table,
			p:     p,
		}
		shardedTablePaginations = append(shardedTablePaginations, shardedTablePagination)
	}

	pageIndex, size := p.Fields().Pagination()
	if pageIndex == 0 && size == 0 {
		listSql, err := p.ListSQL(p.GetTable(), 0, size)
		if err != nil {
			return 0, err
		}
		err = errors.WithMessagef(ErrPaginationSizeRequired, "got sql:%s", listSql)
		return 0, err
	}
	offset := int64(pageIndex * size)
	limit := size
	rvArr := reflect.MakeSlice(rt, 0, 0)
	for _, shardedTablePagination := range shardedTablePaginations {
		if p.withOutTotal && limit <= 0 { // 已满足查询数量，退出循环
			break
		}
		count, err := shardedTablePagination.Count()
		if err != nil {
			return 0, err
		}
		totalCount += count
		if count <= offset {
			offset = offset - count
			continue
		}

		if limit > 0 { // 剩余数量大于0，才执行查询
			subResult := reflect.New(rt).Interface()
			err = shardedTablePagination.List(subResult, int(offset), int(limit))
			if err != nil {
				return 0, err
			}
			beforCount := rvArr.Len()
			rvArr = reflect.AppendSlice(rvArr, reflect.Indirect(reflect.ValueOf(subResult)))
			afterCount := rvArr.Len()
			realCount := uint(afterCount - beforCount) // 获取本次查询的实际数量
			// 更新偏移量与剩余数量
			offset = max(offset-int64(realCount), 0) // 入参pageIndex=0,size=100,实际查到5条，则下一次查询偏移量还是0，只是limit 100-5=95
			limit = limit - realCount

		}
	}
	rv.Set(rvArr)
	return totalCount, nil
}
func (p ShardedTablePaginationParam) ListSQL(tableConfig TableConfig, offset uint, limit uint) (listSQL string, err error) {
	listBuilder := NewListBuilder(tableConfig).WithCustomFieldsFn(p.customFieldsFns...).AppendFields(p._Fields...).WithBuilderFns(p.builderFns...)
	listBuilder = listBuilder.WithBuilderFns(func(ds *goqu.SelectDataset) *goqu.SelectDataset {
		ds = ds.Limit(limit).Offset(offset)
		return ds
	})
	listSql, err := listBuilder.ToSQL()
	if err != nil {
		return "", err
	}
	return listSql, nil
}
