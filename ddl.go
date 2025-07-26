package sqlbuilder

import (
	"fmt"
	"strings"
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

	var columnDefs []string
	var primaryKeys []string
	var uniqueKeys []string
	var indexes []string

	// 字段定义
	for _, col := range tableConfig.Columns {
		colDef := fmt.Sprintf("  `%s` %s", col.DbName, mapGoTypeToMySQL(col.GetType(), col.GetLength()))
		nullable := col.GetNullable()
		if !nullable {
			colDef += " NOT NULL"
		} else {
			colDef += " NULL"
		}
		defaul := col.GetDefault()
		if defaul != nil {
			colDef += " DEFAULT " + escapeDefault(defaul)
		}
		comment := col.GetComment()
		if comment != "" {
			colDef += fmt.Sprintf(" COMMENT '%s'", comment)
		}

		columnDefs = append(columnDefs, colDef)
	}

	// 索引
	for _, idx := range tableConfig.Indexs {
		columnNames := idx.ColumnNames(tableConfig.Columns)
		if len(columnNames) == 0 {
			continue
		}

		escapedCols := make([]string, 0)
		for _, dbName := range columnNames {
			escapedCols = append(escapedCols, fmt.Sprintf("`%s`", dbName))
		}
		indexName := fmt.Sprintf("idx_%s_%s", tableConfig.DBName.Name, strings.Join(columnNames, "_"))
		if idx.IsPrimary {
			primaryKeys = append(primaryKeys, escapedCols...)
		} else if idx.Unique {
			uniqueKeys = append(uniqueKeys, fmt.Sprintf("UNIQUE KEY `uik_%s` (%s)", indexName, strings.Join(escapedCols, ",")))
		} else {
			indexes = append(indexes, fmt.Sprintf("KEY  `ik_%s`(%s)", indexName, strings.Join(escapedCols, ",")))
		}
	}

	// 拼接字段 + 索引
	columnDefs = append(columnDefs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(primaryKeys, ",")))
	columnDefs = append(columnDefs, uniqueKeys...)
	columnDefs = append(columnDefs, indexes...)

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

	var columnDefs []string
	var primaryKeys []string
	var uniqueDefs []string
	var indexes []string // separate CREATE INDEX, outside main DDL

	// 字段定义
	for _, col := range tableConfig.Columns {
		colDef := fmt.Sprintf("  `%s` %s", col.DbName, mapGoTypeToSQLite(col.GetType(), col.GetLength()))
		nullable := col.GetNullable()
		if !nullable {
			colDef += " NOT NULL"
		}
		defaul := col.GetDefault()
		if defaul != nil {
			colDef += " DEFAULT " + escapeDefault(defaul)
		}

		columnDefs = append(columnDefs, colDef)
	}

	// 主键/唯一索引需放到表定义中；普通索引需要 CREATE INDEX
	for _, idx := range tableConfig.Indexs {
		columnNames := idx.ColumnNames(tableConfig.Columns)
		if len(columnNames) == 0 {
			continue
		}

		escapedCols := make([]string, 0, len(columnNames))
		for _, name := range columnNames {
			dbName := ""
			for _, col := range tableConfig.Columns {
				if col.FieldName == name || col.DbName == name {
					dbName = col.DbName
					break
				}
			}
			if dbName != "" {
				escapedCols = append(escapedCols, fmt.Sprintf("`%s`", dbName))
			}
		}

		if idx.IsPrimary {
			primaryKeys = append(primaryKeys, escapedCols...)
		} else if idx.Unique {
			uniqueDefs = append(uniqueDefs, fmt.Sprintf("  UNIQUE (%s)", strings.Join(escapedCols, ",")))
		} else {
			// 普通索引在 SQLite 中要单独 CREATE INDEX
			indexName := fmt.Sprintf("idx_%s_%s", tableConfig.DBName.Name, strings.Join(columnNames, "_"))
			indexStmt := fmt.Sprintf("CREATE INDEX `%s` ON `%s` (%s);", indexName, tableConfig.DBName.Name, strings.Join(escapedCols, ","))
			indexes = append(indexes, indexStmt)
		}
	}

	// 主键
	if len(primaryKeys) > 0 {
		columnDefs = append(columnDefs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(primaryKeys, ",")))
	}
	// 唯一索引
	columnDefs = append(columnDefs, uniqueDefs...)

	sb.WriteString(strings.Join(columnDefs, ",\n"))
	sb.WriteString("\n);")

	ddl = sb.String()
	if len(indexes) > 0 {
		ddl += "\n" + strings.Join(indexes, "\n")
	}

	return ddl, nil
}

// 简化版类型映射（可扩展）
func mapGoTypeToMySQL(t SchemaType, length int) string {
	switch t {
	case "string":
		if length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "TEXT"
	case "int", "int64":
		if length == 0 {
			length = 11
		}
		return fmt.Sprintf("INT(%d)", length)
	case "float", "float64":
		return "DOUBLE"
	case "bool":
		return "BOOLEAN"
	case "time":
		return "DATETIME"
	default:
		return "TEXT"
	}
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
