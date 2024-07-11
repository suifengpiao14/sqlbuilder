package sqlbuilder

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cast"
)

type Column struct {
	Name      string `json:"name"`
	Comment   string `json:"comment"`
	Size      int    `json:"size"`     // 数字大小
	Unsigned  bool   `json:"unsigned"` // 无符号
	Type      string `json:"type"`     // 字符串全部使用string,内部根据MaxLength,MinLength 决定使用哪种数据库字段类型，及大小，同理 int 全部采用 int
	Default   any    `json:"default"`
	Enums     Enums  `json:"enums"`
	MaxLength int    `json:"maxLength"` // 字符串最大长度
	MinLength int    `json:"minLength"` // 字符串最小长度
	Maximum   uint   `json:"maximum"`   // 数字最大值
	Minimum   int    `json:"minimum"`   // 数字最小值
}

type TypeReflect[T int | uint] struct {
	UpperLimit T      `json:"upperLimit"` //上限
	DBType     string `json:"dbType"`     //上限
	IsDefault  bool   `json:"isDefault"`  // 是否为默认类型
	Size       int    `json:"size"`       //大小
}

type TypeReflects[T int | uint] []TypeReflect[T]

func (a TypeReflects[T]) Len() int           { return len(a) }
func (a TypeReflects[T]) Swap(i, j int)      { a[i], a[j].UpperLimit = a[j], a[i].UpperLimit }
func (a TypeReflects[T]) Less(i, j int) bool { return a[i].UpperLimit < a[j].UpperLimit }

func (trs TypeReflects[T]) GetByUpperLimitWithDefault(upperLimit T) (tr *TypeReflect[T]) {
	sort.Sort(trs) // 先排序（从小到大）
	for _, t := range trs {
		if t.UpperLimit >= upperLimit {
			return &t
		}
	}
	tr = trs.GetDefault()
	return tr
}

func (trs TypeReflects[T]) GetByUpperLimit(upperLimit T) (tr *TypeReflect[T], ok bool) {
	sort.Sort(trs) // 先排序（从小到大）
	for _, t := range trs {
		if t.UpperLimit >= upperLimit {
			return &t, true
		}
	}
	return nil, false
}

func (trs TypeReflects[T]) GetDefault() (tr *TypeReflect[T]) {
	sort.Sort(trs) // 先排序（从小到大）
	for _, t := range trs {
		if t.IsDefault {
			return &t
		}
	}
	bigIndex := len(trs) - 1
	if bigIndex > -1 {
		return &trs[bigIndex]
	}
	return nil
}

var TypeReflectsString = TypeReflects[int]{
	{UpperLimit: 64, DBType: "char"},                      // 小于64位往往是ID、日期等类型，长度较为固定，直接使用char 效率高
	{UpperLimit: 255, DBType: "varchar", IsDefault: true}, // 小字符串类型，节省空间
	{UpperLimit: 65535, DBType: "TEXT"},
	{UpperLimit: 16777215, DBType: "MEDIUMTEXT"},
	{UpperLimit: 4294967295, DBType: "LONGTEXT"},
}

const (
	UnsinedInt_maximum_tinyint   = 1<<8 - 1
	UnsinedInt_maximum_smallint  = 1<<16 - 1
	UnsinedInt_maximum_mediumint = 1<<24 - 1
	UnsinedInt_maximum_int       = 1<<32 - 1
	UnsinedInt_maximum_bigint    = 1<<64 - 1

	Int_maximum_tinyint   = 1<<7 - 1
	Int_maximum_smallint  = 1<<15 - 1
	Int_maximum_mediumint = 1<<23 - 1
	Int_maximum_int       = 1<<31 - 1
	Int_maximum_bigint    = 1<<63 - 1
)

// 无符号整型
var TypeReflectsUnsinedInt = TypeReflects[uint]{
	{UpperLimit: 1<<8 - 1, DBType: "TINYINT", Size: 1},
	{UpperLimit: 1<<16 - 1, DBType: "SMALLINT", Size: 11},
	{UpperLimit: 1<<24 - 1, DBType: "mediumint", Size: 11},
	{UpperLimit: 1<<32 - 1, DBType: "int", Size: 11, IsDefault: true},
	{UpperLimit: 1<<64 - 1, DBType: "bigint", Size: 11},
}
var TypeReflectsInt = TypeReflects[int]{
	{UpperLimit: 1<<7 - 1, DBType: "TINYINT", Size: 1},
	{UpperLimit: 1<<15 - 1, DBType: "SMALLINT", Size: 11},
	{UpperLimit: 1<<23 - 1, DBType: "mediumint", Size: 11},
	{UpperLimit: 1<<31 - 1, DBType: "int", Size: 11, IsDefault: true},
	{UpperLimit: 1<<63 - 1, DBType: "bigint", Size: 11},
}

func (col *Column) DDL(driver Driver) (ddl string) {
	notNil := ""
	defaul := col.Default
	comment := ""
	if col.Comment != "" {
		comment = fmt.Sprintf(`COMMENT "%s"`, col.Comment)
	}
	if col.Enums != nil {
		col.Type = col.Enums.Type()
		col.MaxLength, col.Maximum = col.Enums.MaxLengthMaximum()
	}
	typ := col.Type
	switch col.Type {
	case "string":
		if col.MaxLength < 1 {
			col.MaxLength = 255
		}
		if defaul == nil {
			defaul = `""`
		}
		tr := TypeReflectsString.GetByUpperLimitWithDefault(col.MaxLength)
		if tr != nil {
			typ = fmt.Sprintf("%s(%d)", tr.DBType, col.MaxLength)
		}
	case "int":
		if col.Size < 1 {
			col.Size = 11
		}
		if defaul == nil {
			defaul = 0
		}
		if col.Unsigned {
			tr := TypeReflectsUnsinedInt.GetByUpperLimitWithDefault(col.Maximum)
			if tr != nil {
				typ = fmt.Sprintf("%s(%d) unsigned", tr.DBType, col.Size)
			}
		} else {
			tr := TypeReflectsInt.GetByUpperLimitWithDefault(int(col.Maximum))
			if tr != nil {
				typ = fmt.Sprintf("%s(%d)", tr.DBType, col.Size)
			}
		}

	default:
		typ = col.Type
	}
	defaulStr := ""
	if defaul != nil {
		defaulStr = fmt.Sprintf("default %s", cast.ToString(defaul))
		notNil = " not null "
	}

	ddl = fmt.Sprintf("%s %s %s %s %s", col.Name, typ, notNil, defaulStr, comment)
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
