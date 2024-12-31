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
		val := `"\n\t<div x-data='$tab({tab_eventName:\"\",tab_activeTabId:\"\"})' >\n    <div role=\"tablist\" class=\"tabs tabs-bordered \">\n      <div class=\"w-full border-t-2\">\n\t\t<a tab-for=\"\" x-bind=\"bind_tab\" role=\"tab\" class=\"tab px-1\">到店单</a>\n\t\t<a tab-for=\"\" x-bind=\"bind_tab\" role=\"tab\" class=\"tab px-1\">上门单tab</a>\n      </div>\n      <div class=\"w-full \">\n\t\t\t<div x-cloak id=\"\" x-bind=\"bind_tabpanel\" role=\"tabpanel\" class=\"tab-content bg-base-100 border-base-300  w-full\">\n\t\t\t\t\n\t\t\t</div>\n\t\t\t<div x-cloak id=\"\" x-bind=\"bind_tabpanel\" role=\"tabpanel\" class=\"tab-content bg-base-100 border-base-300  w-full\">\n\t\t\t\t\n\t\t\t</div>\n      </div>\n\t</div>\n    </div>\n"`
		val = sqlbuilder.MysqlEscapeString(val)
		fmt.Println(val)
	})

	t.Run("已经转义", func(t *testing.T) {
		val := "\n\t<div x-data='$tab({tab_eventName:\"\",tab_activeTabId:\"\"})' >\n    <div role=\"tablist\" class=\"tabs tabs-bordered \">\n      <div class=\"w-full border-t-2\">\n\t\t<a tab-for=\"tab-2-0\" x-bind=\"bind_tab\" role=\"tab\" class=\"tab px-1\">到店单</a>\n\t\t<a tab-for=\"tab-2-1\" x-bind=\"bind_tab\" role=\"tab\" class=\"tab px-1\">上门单</a>\n      </div>\n      <div class=\"w-full \">\n\t\t\t<div x-cloak id=\"tab-2-0\" x-bind=\"bind_tabpanel\" role=\"tabpanel\" class=\"tab-content bg-base-100 border-base-300  w-full\">\n\t\t\t\t到店单 内容\n\t\t\t</div>\n\t\t\t<div x-cloak id=\"tab-2-1\" x-bind=\"bind_tabpanel\" role=\"tabpanel\" class=\"tab-content bg-base-100 border-base-300  w-full\">\n\t\t\t\t上门单 内容\n\t\t\t</div>\n      </div>\n\t</div>\n    </div>\n"
		val = sqlbuilder.MysqlEscapeString(val)
		fmt.Println(val)
	})

}
