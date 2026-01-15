package sqlbuilder_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlbuilder"
)

func NewAnyAmount(isAuto int) *sqlbuilder.Field {
	return sqlbuilder.NewIntField(isAuto, "isAuto", "是否自定义金额1-是,0-否", 0)
}

func NewNotifyUrl(notifyUrl string) *sqlbuilder.Field {
	return sqlbuilder.NewStringField(notifyUrl, "notifyUrl", "支付成功回调地址", 0)
}

func NewOrderId(orderId string) *sqlbuilder.Field {
	return sqlbuilder.NewStringField(orderId, "orderId", "订单号", 0)
}

var table = sqlbuilder.NewTableConfig("pay_order_1").WithHandler(sqlbuilder.NewGormHandler(sqlbuilder.DB2Gorm(sqlbuilder.GetDB, nil))).AddColumns(
	sqlbuilder.NewColumn("order_id", sqlbuilder.GetField(NewOrderId)),
	sqlbuilder.NewColumn("notify_url", sqlbuilder.GetField(NewNotifyUrl)),
	sqlbuilder.NewColumn("isAuto", sqlbuilder.GetField(NewAnyAmount)),
).AddIndexs(
	sqlbuilder.Index{
		IsPrimary: true,
		ColumnNames: func(table sqlbuilder.TableConfig) (columnNames []string) {
			return []string{table.GetDBNameByFieldNameMust(sqlbuilder.GetFieldName(NewOrderId))}
		},
	},
).WithComment("支付订单表")

func init() {
	sqlbuilder.CreateTableIfNotExists = true
}
func TestGenerateDDL(t *testing.T) {
	ddl, err := table.GenerateDDL()
	require.NoError(t, err)
	fmt.Println("ddl:\n", ddl)
}

func TestCrateTable(t *testing.T) {
	table.GetHandlerWithInitTable()
}
