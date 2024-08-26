package sqlbuilder_test

import (
	"fmt"
	"testing"

	"github.com/suifengpiao14/sqlbuilder"
)

func TestMysqlRealEscapeString(t *testing.T) {
	str := `{   "originalMessage" : "{   \"app_key\" : \"34657042\",   \"biz_order_id\" : \"881723450001036535\",   \"order_status\" : \"5\",   \"order_sub_status\" : \"50\"}",   "type" : "4"}`
	val := sqlbuilder.MysqlEscapeString(str)
	fmt.Println(val)
}
