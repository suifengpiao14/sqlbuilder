package sqlbuilder

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/spf13/cast"
)

type Column struct {
	Name      string `json:"name"`
	Comment   string `json:"comment"`
	Type      string `json:"type"`
	Default   any    `json:"default"`
	Enums     Enums  `json:"enums"`
	MaxLength int    `json:"maxLength"` // 字符串最大长度
	MinLength int    `json:"minLength"` // 字符串最小长度
	Maximum   int    `json:"maximum"`   // 数字最大值
	Minimum   int    `json:"minimum"`   // 数字最小值
}

func (col *Column) DDL(driver Driver) (ddl string) {
	notNil := ""
	defaul := ""
	if !IsNil(col.Default) {
		defaul = fmt.Sprintf("default %s", cast.ToString(col.Default))
	}
	if defaul != "" {
		notNil = " not null "
	}
	comment := ""
	if col.Comment != "" {
		comment = fmt.Sprintf(`COMMENT "%s"`, col.Comment)
	}

	ddl = fmt.Sprintf("%s %s %s %s %s", col.Name, col.Type, notNil, defaul, comment)
	return ddl
}

type Columns []Column

func (cols Columns) DDL(driver Driver) (ddl string) {
	lines := make([]string, 0)
	for _, col := range cols {
		lines = append(lines, col.DDL(driver))
	}
	ddl = strings.Join(lines, ",\n")
	ddl = fmt.Sprintf("%s\n", ddl)
	return ddl
}

type Table struct {
	_Driver  Driver
	_Columns Columns // 这里占时记录列名称，但是实际上不够
	Comment  string
}

func (t *Table) GetTable() string {
	return ""
}

func (t *Table) DDL() (ddl string) {
	var w bytes.Buffer
	w.WriteString(fmt.Sprintf(" CREATE TABLE IF NOT EXISTS `%s`(\n", t.GetTable()))
	w.WriteString(t.GetColumns().DDL(t.GetDriver()))
	w.WriteString("\n")
	w.WriteString(fmt.Sprintf(`ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT="%s";`, t.Comment))
	return w.String()
}
func (t *Table) SetColumns(columns ...Column) {
	t._Columns = columns
}

func (t *Table) GetColumns() (columns Columns) {
	return t._Columns
}

func (t *Table) SetDriver(driver Driver) {
	t._Driver = driver
}

func (t *Table) GetDriver() (Driver Driver) {
	return t._Driver
}
