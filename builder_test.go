package sqlbuilder_test

import (
	"fmt"
	"testing"

	"github.com/suifengpiao14/sqlbuilder"
)

func TestMysqlRealEscapeString(t *testing.T) {
	t.Run("双引号", func(t *testing.T) {
		str := `{   "originalMessage" : "{   \"app_key\" : \"34657042\",   \"biz_order_id\" : \"881723450001036535\",   \"order_status\" : \"5\",   \"order_sub_status\" : \"50\"}",   "type" : "4"}`
		val := sqlbuilder.MysqlEscapeString(str)
		fmt.Println(val)
	})

	t.Run("单引号", func(t *testing.T) {
		val := `x-data='$tab({"eventName":"hello_world"})'`
		val = sqlbuilder.MysqlEscapeString(val)
		fmt.Println(val)
	})

}
