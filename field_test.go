package sqlbuilder_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/funcs"
	"github.com/suifengpiao14/sqlbuilder"
)

func TestCopy(t *testing.T) {
	f1 := &sqlbuilder.Field{
		Name: "old name",
		Schema: &sqlbuilder.Schema{
			Type: "old type",
			Enums: sqlbuilder.Enums{
				{Key: "old", Title: "复制前的"},
			},
		},
	}
	cpF := f1.Copy()
	f1.Name = "new name"
	f1.Schema.Type = "new type"
	f1.Schema.Enums.Append(sqlbuilder.Enum{Key: "new", OrderDesc: 100, Title: "复制后改动"})
	sort.Sort(f1.Schema.Enums)

	fmt.Println(f1.Name, f1.Schema.Type, f1.Select())
	fmt.Println(cpF.Name, cpF.Schema.Type, cpF.Select())

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
	inputValue := ""
	b, err := json.Marshal(data)
	if err != nil {
		inputValue = err.Error()
	} else {
		inputValue = string(b)
	}
	return sqlbuilder.NewField(inputValue).SetName("data").SetTitle("返回数据").SetType(sqlbuilder.Schema_doc_Type_null)
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
				docName := f.Name
				if docName != "" && !strings.HasPrefix(docName, "[]") {
					docName = fmt.Sprintf(".%s", docName)
				}
				getJsonTag := getJsonTag(structField)
				fName := fmt.Sprintf("%s%s", getJsonTag, docName)
				fName = strings.TrimSuffix(fName, ".")
				f.SetName(fName)
			}
		}
	}
	return fs
}

func ArrayFieldCustom(fs sqlbuilder.Fields) sqlbuilder.Fields {
	for _, f := range fs {
		fName := fmt.Sprintf("[].%s", f.Name)
		fName = strings.TrimSuffix(fName, ".")
		f.SetName(fName) //设置列名称,f 本身为指针，直接修改f.Name
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

func TestDataIsNil(t *testing.T) {
	f := sqlbuilder.NewIntField(0, "id", "ID", 0)
	f.ValueFns.Append(sqlbuilder.ValueFnEmpty2Nil)
	fs := sqlbuilder.Fields{f}
	data, err := fs.Data()
	require.NoError(t, err)
	isNil := sqlbuilder.IsNil(data)
	fmt.Println(isNil)
}

func TestValueFns(t *testing.T) {
	valueFns := sqlbuilder.ValueFns{
		sqlbuilder.ValueFn{
			Name:  "second",
			Order: 2,
		},
		sqlbuilder.ValueFn{
			Name:  "third",
			Order: 3,
		},
		sqlbuilder.ValueFn{
			Name:  "first",
			Order: 1,
		},
	}
	valueFns.Sort()
	require.Equal(t, valueFns[0].Name, "first")
}

func TestPopFirstMain(t *testing.T) {
	table1 := sqlbuilder.TableConfig{
		DBName: sqlbuilder.DBName{Name: "test1"},
	}
	table2 := sqlbuilder.TableConfig{
		DBName: sqlbuilder.DBName{Name: "test1"},
	}

	field := sqlbuilder.NewIntField(0, "id", "ID", 0)
	on := sqlbuilder.NewOn(
		sqlbuilder.OnUnit{Table: table1, Field: field},
		sqlbuilder.OnUnit{Table: table2, Field: field},
	)
	table, _ := on.Condition()
	require.Equal(t, table2.AliasOrTableExpr(), table)
}

func TestAddAlias(t *testing.T) {
	f := &sqlbuilder.Field{}
	f2 := sqlbuilder.Field{}
	fWithAlais := f.AddAlias(&f2)
	fmt.Println(fWithAlais.GetAlias())
}
