package sqlbuilder

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

var (
	CREATE_TABLE_IF_NOT_EXISTS = false // 是否在创建表时检查该表是否存在，如果不存在则创建,开发环境建议设置为true，生产环境建议设置为false
)

func (tableConfig TableConfig) GenerateDDL() (ddl string, err error) {
	if Dialect.IsMysql() {
		return tableConfig.generateMysqlDDL()
	} else {
		return tableConfig.generateSQLite3DDL()
	}

}
func (tableConfig TableConfig) generateMysqlDDL() (ddl string, err error) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n", tableConfig.DBName.Name))

	// 字段定义
	columnDefs := tableConfig.Columns.DDL(Driver_mysql)
	// 索引
	indexDefs := tableConfig.Indexs.DDL(Driver_mysql, tableConfig)

	columnDefs = append(columnDefs, indexDefs...)

	sb.WriteString(strings.Join(columnDefs, ",\n"))
	sb.WriteString("\n) ENGINE=InnoDB AUTO_INCREMENT=1  DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ")
	if tableConfig.Comment != "" {
		sb.WriteString(fmt.Sprintf(` COMMENT ="%s";`, tableConfig.Comment))
		sb.WriteString("\n")
	}

	return sb.String(), nil
}

func (tableConfig TableConfig) generateSQLite3DDL() (ddl string, err error) {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CREATE TABLE `%s` (\n", tableConfig.DBName.Name))

	// 字段定义
	columnDefs := tableConfig.Columns.DDL(Driver_sqlite3)
	// 索引
	indexDefs := tableConfig.Indexs.DDL(Driver_sqlite3, tableConfig)

	columnDefs = append(columnDefs, indexDefs...)

	sb.WriteString(strings.Join(columnDefs, ",\n"))
	sb.WriteString("\n);")
	ddl = sb.String()
	return ddl, nil
}

func (cols ColumnConfigs) DDL(driver Driver) (lines []string) {
	arr := make([]string, 0)
	for _, col := range cols {
		arr = append(arr, col.DDL(driver))
	}

	lines = make([]string, 0)
	for _, l := range arr {
		if strings.TrimSpace(l) == "" {
			continue
		}
		lines = append(lines, l)
	}
	return lines
}

func (col ColumnConfig) DDL(driver Driver) (ddl string) {
	col = col.CopyFieldSchemaIfEmpty()
	switch driver {
	case Driver_mysql:
		return Column2DDLMysql(col)
	case Driver_sqlite3:
		return Column2DDLSQLite(col)
	}
	err := errors.Errorf("unsport driver:%s", string(driver))
	panic(err)
}

func (indexs Indexs) DDL(driver Driver, table TableConfig) (lines []string) {
	arr := make([]string, 0)
	for _, index := range indexs {
		arr = append(arr, index.DDL(driver, table))
	}

	lines = make([]string, 0)
	for _, l := range arr {
		if strings.TrimSpace(l) == "" {
			continue
		}
		lines = append(lines, l)
	}
	return lines

}

func (index Index) DDL(driver Driver, table TableConfig) (line string) {
	switch driver {
	case Driver_mysql:
		return Index2DDLMysql(index, table)
	case Driver_sqlite3:
		return Index2DDLSQLite(index, table)
	}
	err := errors.Errorf("unsport driver:%s", string(driver))
	panic(err)
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
