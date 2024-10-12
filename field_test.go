package sqlbuilder_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/sqlbuilder"
)

func TestCopy(t *testing.T) {
	f1 := &sqlbuilder.Field{
		Name: "f1",
		Schema: &sqlbuilder.Schema{
			Type: "f1",
		},
	}
	f2 := *f1
	f1.SetSelectColumns("f1")
	f2.Name = "f2"
	schema := *f1.Schema
	f2.Schema = &schema
	f2.Schema.Type = "f2"

	fmt.Println(f1.Name, f1.Schema.Type, f1.Select())
	fmt.Println(f2.Name, f2.Schema.Type, f2.Select())

}

type PaginationOut struct {
	Pagination Pagination `json:"pagination"`
}

func TestFieldStructToArray(t *testing.T) {
	out := ErrorOut{
		Data: PaginationOut{},
	}
	respFields := sqlbuilder.StructToFields(out, StructFieldCustom, ArrayFieldCustom)
	fmt.Println(respFields)
}

type ErrorOut struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

func (e ErrorOut) Fields() sqlbuilder.Fields {
	return sqlbuilder.Fields{
		NewCode(e.Code),
		NewMessage(e.Message),
		NewData(e.Data),
	}
}

type Pagination struct {
	PageIndex string `json:"pageIndex"`
	PageSize  string `json:"pageSize"`
	Total     string `json:"total"`
}

func (p Pagination) Fields() sqlbuilder.Fields {
	return sqlbuilder.Fields{
		NewPageIndex(0),
		NewPageSize(0),
		NewTotal(0),
	}
}

func NewCode(code string) *sqlbuilder.Field {
	return sqlbuilder.NewStringField(code, "code", "错误编码", 20).SetDescription("0-正常,其它-异常")
}
func NewMessage(message string) *sqlbuilder.Field {
	return sqlbuilder.NewStringField(message, "message", "错误信息", 1024)
}
func NewData(data any) *sqlbuilder.Field {
	return sqlbuilder.NewField(func(_ any) (any, error) {
		if data == nil {
			return nil, nil
		}
		b, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}

		inputValue := string(b)
		return inputValue, nil
	}).SetName("data").SetTitle("返回数据").SetType(sqlbuilder.Schema_doc_Type_null)
}

func NewPageIndex(pageIndex int) *sqlbuilder.Field {
	return sqlbuilder.NewIntField(pageIndex, "pageIndex", "页面", 0)
}
func NewPageSize(pageSize int) *sqlbuilder.Field {
	return sqlbuilder.NewIntField(pageSize, "pageSize", "每页数量", 100)
}

func NewTotal(total int) *sqlbuilder.Field {
	return sqlbuilder.NewIntField(total, "total", "总数", 0)
}

func StructFieldCustom(val reflect.Value, structField reflect.StructField, fs sqlbuilder.Fields) sqlbuilder.Fields {
	for _, f := range fs {
		f.SetFieldName(funcs.ToLowerCamel(structField.Name)) //设置列名称
	}
	switch structField.Type.Kind() {
	case reflect.Array, reflect.Slice, reflect.Struct:
		if !structField.Anonymous { // 嵌入结构体,文档名称不增加前缀
			for i := 0; i < len(fs); i++ {
				f := fs[i]
				docName := f.GetDocName()
				if docName != "" && !strings.HasPrefix(docName, "[]") {
					docName = fmt.Sprintf(".%s", docName)
				}
				getJsonTag := getJsonTag(structField)
				fName := fmt.Sprintf("%s%s", getJsonTag, docName)
				fName = strings.TrimSuffix(fName, ".")
				f.SetDocName(fName)
			}
		}
	}
	return fs
}

func ArrayFieldCustom(fs sqlbuilder.Fields) sqlbuilder.Fields {
	for _, f := range fs {
		fName := fmt.Sprintf("[].%s", f.GetDocName())
		fName = strings.TrimSuffix(fName, ".")
		f.SetDocName(fName) //设置列名称,f 本身为指针，直接修改f.Name
	}
	return fs
}

func getJsonTag(val reflect.StructField) (jsonTag string) {
	tag := val.Tag.Get("json")
	if tag == "-" {
		tag = ""
	}
	return tag
}

func TestIsGenericByFieldFn(t *testing.T) {
	var a sqlbuilder.FieldFn[int]
	ok := sqlbuilder.IsGenericByFieldFn(reflect.TypeOf(a))
	fmt.Println(ok)

}
