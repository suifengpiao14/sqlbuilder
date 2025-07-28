package sqlbuilder

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

var (
	CREATE_TABLE_IF_NOT_EXISTS = false // 是否在创建表时检查该表是否存在，如果不存在则创建,开发环境建议设置为true，生产环境建议设置为false
)

func (tableConfig TableConfig) GenerateDDL() (ddl string, err error) {
	return GenerateDDL(Driver(Dialect.dialect), tableConfig)
}

func GenerateDDL(driver Driver, tableConfig TableConfig) (ddl string, err error) {
	// 字段、索引定义
	columnDefs, err := MakeColumnsAndIndexs(Driver_mysql, tableConfig)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	switch driver {
	case Driver_mysql:
		sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n", tableConfig.DBName.Name))
		sb.WriteString(strings.Join(columnDefs, ",\n"))
		sb.WriteString("\n) ENGINE=InnoDB AUTO_INCREMENT=1  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ")
		if tableConfig.Comment != "" {
			sb.WriteString(fmt.Sprintf(` COMMENT ="%s";`, tableConfig.Comment))

		}
	case Driver_sqlite3:
		sb.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", tableConfig.DBName.Name))
		sb.WriteString(strings.Join(columnDefs, ",\n"))
	default:
		err := errors.Errorf("unsport driver:%s", string(driver))
		return "", err
	}
	sb.WriteString("\n")
	return sb.String(), nil
}

func MakeColumnsAndIndexs(driver Driver, table TableConfig) (lines []string, err error) {
	arr := make([]string, 0)
	switch driver {
	case Driver_mysql:
		for _, col := range table.Columns {
			col = col.CopyFieldSchemaIfEmpty()
			ddl := Column2DDLMysql(col)
			if strings.TrimSpace(ddl) != "" {
				arr = append(arr, ddl)
			}

		}
		for _, index := range table.Indexs {
			ddl := Index2DDLMysql(index, table)
			if strings.TrimSpace(ddl) != "" {
				arr = append(arr, ddl)
			}
		}

	case Driver_sqlite3:
		for _, col := range table.Columns {
			col = col.CopyFieldSchemaIfEmpty()
			ddl := Column2DDLSQLite(col)
			if strings.TrimSpace(ddl) != "" {
				arr = append(arr, ddl)
			}
		}
		for _, index := range table.Indexs {
			ddl := Index2DDLSQLite(index, table)
			if strings.TrimSpace(ddl) != "" {
				arr = append(arr, ddl)
			}
		}
	default:
		err := errors.Errorf("unsport driver:%s", string(driver))
		return nil, err
	}

	return arr, nil

}

func Column2DDLSQLite(col ColumnConfig) (ddl string) {
	colDef := fmt.Sprintf("  `%s` %s", col.DbName, mapGoTypeToSQLite(col.Type, col.Length))
	if col.NotNull {
		colDef += " NOT NULL"
	}
	if col.Default != nil {
		colDef += " DEFAULT " + escapeDefault(col.Default)
	}
	return colDef
}

func Index2DDLSQLite(index Index, table TableConfig) (ddl string) {
	columnNames := index.ColumnNames(table.Columns)
	if len(columnNames) == 0 {
		return ""
	}

	escapedCols := make([]string, 0, len(columnNames))
	for _, name := range columnNames {
		escapedCols = append(escapedCols, fmt.Sprintf("`%s`", name))
	}

	if index.IsPrimary {
		ddl = fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(escapedCols, ","))
	} else if index.Unique {
		ddl = fmt.Sprintf("  UNIQUE (%s)", strings.Join(escapedCols, ","))
	} else {
		// 普通索引在 SQLite 中要单独 CREATE INDEX
		indexName := fmt.Sprintf("idx_%s", strings.Join(columnNames, "_"))
		ddl = fmt.Sprintf("CREATE INDEX `%s` ON `%s` (%s);", indexName, table.DBName.Name, strings.Join(escapedCols, ","))
	}
	return ddl
}

func Index2DDLMysql(index Index, table TableConfig) (ddl string) {
	columnNames := index.ColumnNames(table.Columns)
	if len(columnNames) == 0 {
		return ""
	}

	escapedCols := make([]string, 0)
	for _, dbName := range columnNames {
		escapedCols = append(escapedCols, fmt.Sprintf("`%s`", dbName))
	}

	indexName := fmt.Sprintf("idx_%s", strings.Join(columnNames, "_"))
	switch {
	case index.IsPrimary:
		ddl = fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(escapedCols, ","))
	case index.Unique:
		ddl = fmt.Sprintf("UNIQUE KEY `uik_%s` (%s)", indexName, strings.Join(escapedCols, ","))
	default:
		ddl = fmt.Sprintf("KEY  `ik_%s`(%s)", indexName, strings.Join(escapedCols, ","))
	}

	return ddl
}

func Column2DDLMysql(col ColumnConfig) (ddl string) {
	if col.Enums != nil {
		col.Type = SchemaType(col.Enums.Type())
		maxLength, maximum := col.Enums.MaxLengthMaximum()
		col.Length = max(maxLength, int(maximum))
		col.Default = col.Enums.Default().Key
	}

	notNil := ""
	comment := ""
	if col.Comment != "" {
		comment = fmt.Sprintf(`COMMENT "%s"`, col.Comment)
	}
	defaul := col.Default

	typ := col.Type.String()
	switch col.Type.String() {
	case "string":
		if col.Length == 0 {
			col.Length = 255
		}
		defaul = fmt.Sprintf(`"%s"`, cast.ToString(defaul)) // 增加引号
		tr := TypeReflectsString.GetByUpperLimitWithDefault(col.Length)
		if tr != nil {
			typ = tr.DBType
			if tr.Size > -1 {
				typ = fmt.Sprintf("%s(%d)", typ, col.Length)
			}
			if tr.NoDefaultValue {
				defaul = nil // 不容许设置默认值
			}
		}
	case "int":
		if col.Length < 1 {
			col.Length = 11
		}
		if defaul == nil {
			defaul = 0
		}
		if col.Unsigned {
			tr := TypeReflectsUnsinedInt.GetByUpperLimitWithDefault(uint(col.Length))
			if tr != nil {
				typ = fmt.Sprintf("%s(%d) unsigned", tr.DBType, col.Length)
			}
		} else {
			tr := TypeReflectsInt.GetByUpperLimitWithDefault(col.Length)
			if tr != nil {
				typ = fmt.Sprintf("%s(%d)", tr.DBType, col.Length)
			}
		}
	default:
		typ = col.Type.String()
	}
	defaulStr := ""
	if defaul != nil {
		defaulStr = fmt.Sprintf("default %s", cast.ToString(defaul))

	}

	autoIncrement := ""
	if col.AutoIncrement {
		autoIncrement = "AUTO_INCREMENT"
		defaulStr = "" // 自增不需要默认值
	}

	if col.Tags.HastTag(Tag_createdAt) {
		typ = "datetime"
		defaulStr = "default CURRENT_TIMESTAMP"
	} else if col.Tags.HastTag(Tag_updatedAt) {
		typ = "datetime"
		defaulStr = "default  CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
	}
	if defaul != nil {
		notNil = " not null "
	}
	ddl = fmt.Sprintf("%s %s %s %s %s %s", col.DbName, typ, notNil, autoIncrement, defaulStr, comment)
	return ddl
}

type TypeReflect[T int | uint] struct {
	UpperLimit     T      `json:"upperLimit"`     //上限
	DBType         string `json:"dbType"`         //上限
	IsDefault      bool   `json:"isDefault"`      // 是否为默认类型
	Size           int    `json:"size"`           //大小
	NoDefaultValue bool   `json:"noDefaultValue"` //不可设置默认值
}

type TypeReflects[T int | uint] []TypeReflect[T]

func (trs TypeReflects[T]) Len() int           { return len(trs) }
func (trs TypeReflects[T]) Swap(i, j int)      { trs[i], trs[j].UpperLimit = trs[j], trs[i].UpperLimit }
func (trs TypeReflects[T]) Less(i, j int) bool { return trs[i].UpperLimit < trs[j].UpperLimit }

func (trs TypeReflects[T]) GetByUpperLimitWithDefault(upperLimit T) (tr *TypeReflect[T]) {
	if upperLimit > 0 {
		sort.Sort(trs) // 先排序（从小到大）
		for _, t := range trs {
			if t.UpperLimit >= upperLimit {
				return &t
			}
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
	{UpperLimit: 64, DBType: "char"},                                             // 小于64位往往是ID、日期等类型，长度较为固定，直接使用char 效率高
	{UpperLimit: 255, DBType: "varchar", IsDefault: true},                        // 小字符串类型，节省空间
	{UpperLimit: 65535, DBType: "TEXT", Size: -1, NoDefaultValue: true},          // size =-1 不设置大小
	{UpperLimit: 16777215, DBType: "MEDIUMTEXT", Size: -1, NoDefaultValue: true}, // size =-1 不设置大小
	{UpperLimit: 4294967295, DBType: "LONGTEXT", Size: -1, NoDefaultValue: true}, // size =-1 不设置大小
}

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

func escapeDefault(val any) string {
	switch v := val.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}

func mapGoTypeToSQLite(t SchemaType, _ int) string {
	switch t {
	case "string":
		return "TEXT"
	case "int", "int64":
		return "INTEGER"
	case "float", "float64":
		return "REAL"
	case "bool":
		return "BOOLEAN"
	case "time":
		return "DATETIME"
	default:
		return "TEXT"
	}
}
