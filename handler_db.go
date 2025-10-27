package sqlbuilder

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
)

// 针对实现了FieldsI 接口的模型，优先使用SqlDBHandler 处理，能减少硬编码
type SqlDBHandler func() *sql.DB

func NewFieldIDBHandler(getDB func() *sql.DB) Handler {
	return SqlDBHandler(getDB)
}

func (h SqlDBHandler) Transaction(fc func(tx Handler) error, opts ...*sql.TxOptions) (err error) {
	db := h()
	ctx := context.Background()
	var opt *sql.TxOptions
	for i := range opts {
		opt = opts[i]
	}

	tx, err := db.BeginTx(ctx, opt)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	txHandler := NewTxHandler(db, tx)
	err = fc(txHandler)
	if err != nil {
		return err
	}
	_ = tx.Commit()
	return err
}
func (h SqlDBHandler) GetDialector() string {
	return DetectDriver(h())
}
func (h SqlDBHandler) GetSqlDBHandler() SqlDBHandler {
	return h
}
func (h SqlDBHandler) OriginalHandler() Handler {
	return h
}
func (h SqlDBHandler) IsOriginalHandler() bool {
	return true
}

func (h SqlDBHandler) Exec(sql string) (err error) {
	_, err = h().Exec(sql)
	if err != nil {
		return err
	}

	return nil
}
func (h SqlDBHandler) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
	result, err := h().Exec(sql)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func (h SqlDBHandler) Exists(sql string) (exists bool, err error) {
	result := make([]any, 0)
	ctx := context.Background()
	err = h.Query(ctx, sql, &result)
	if err != nil {
		return false, err
	}
	exists = len(result) > 0
	return exists, nil
}
func (h SqlDBHandler) InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error) {

	result, err := h().Exec(sql)
	if err != nil {
		return 0, 0, err
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	lastInsertIdi, err := result.LastInsertId()
	if err != nil {
		return 0, 0, err
	}
	lastInsertId = uint64(lastInsertIdi)
	return lastInsertId, rowsAffected, nil
}
func (h SqlDBHandler) First(_ context.Context, sqlstr string, result any) (exists bool, err error) {
	rows, err := h().Query(sqlstr)
	if err != nil {
		return false, err
	}
	rowsAffected, err := Scan(rows, result)
	if err != nil {
		return false, err
	}
	exists = rowsAffected > 0
	return exists, nil
}

func (h SqlDBHandler) Query(_ context.Context, sqlstr string, result any) (err error) {
	rows, err := h().Query(sqlstr)
	if err != nil {
		return err
	}
	_, err = Scan(rows, result)
	if err != nil {
		return err
	}
	return nil
}

func (h SqlDBHandler) Count(sql string) (count int64, err error) {
	err = h().QueryRow(sql).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (h SqlDBHandler) GetDB() *sql.DB {
	return h()
}

type _TxHandler struct {
	db *sql.DB
	tx *sql.Tx
}

func NewTxHandler(db *sql.DB, tx *sql.Tx) Handler {
	return _TxHandler{
		db: db,
		tx: tx,
	}
}

func (h _TxHandler) Transaction(fc func(tx Handler) error, opts ...*sql.TxOptions) (err error) {
	err = fc(h)
	if err != nil {
		return err
	}
	return err
}
func (h _TxHandler) GetDialector() string {
	return detectDriverTx(h.tx)
}
func (h _TxHandler) GetSqlDBHandler() SqlDBHandler {
	err := errors.Errorf("事务中不能再重新获取数据库句柄，避免循环调用，导致死锁")
	panic(err)
}
func (h _TxHandler) OriginalHandler() Handler {
	return h
}
func (h _TxHandler) IsOriginalHandler() bool {
	return true
}

func (h _TxHandler) Exec(sql string) (err error) {
	_, err = h.tx.Exec(sql)
	if err != nil {
		return err
	}

	return nil
}
func (h _TxHandler) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
	result, err := h.tx.Exec(sql)
	if err != nil {
		return 0, err
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return rowsAffected, nil
}

func (h _TxHandler) Exists(sql string) (exists bool, err error) {
	result := make([]any, 0)
	ctx := context.Background()
	err = h.Query(ctx, sql, &result)
	if err != nil {
		return false, err
	}
	exists = len(result) > 0
	return exists, nil
}
func (h _TxHandler) InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error) {

	result, err := h.tx.Exec(sql)
	if err != nil {
		return 0, 0, err
	}
	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return 0, 0, err
	}
	lastInsertIdi, err := result.LastInsertId()
	if err != nil {
		return 0, 0, err
	}
	lastInsertId = uint64(lastInsertIdi)
	return lastInsertId, rowsAffected, nil
}
func (h _TxHandler) First(_ context.Context, sqlstr string, result any) (exists bool, err error) {
	rows, err := h.tx.Query(sqlstr)
	if err != nil {
		return false, err
	}
	rowsAffected, err := Scan(rows, result)
	if err != nil {
		return false, err
	}
	exists = rowsAffected > 0
	return exists, nil
}

func (h _TxHandler) Query(_ context.Context, sqlstr string, result any) (err error) {
	rows, err := h.tx.Query(sqlstr)
	if err != nil {
		return err
	}
	_, err = Scan(rows, result)
	if err != nil {
		return err
	}
	return nil
}

func (h _TxHandler) Count(sql string) (count int64, err error) {
	err = h.tx.QueryRow(sql).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (h _TxHandler) GetDB() *sql.Tx {
	return h.tx
}

// ScanMode 用于控制扫描行为
type ScanMode uint8

const (
	ScanInitialized         ScanMode = 1 << 0
	ScanUpdate              ScanMode = 1 << 1
	ScanOnConflictDoNothing ScanMode = 1 << 2
)

type Rows interface {
	Columns() ([]string, error)
	ColumnTypes() ([]*sql.ColumnType, error)
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
	Close() error
}

// Scan 支持 map / slice / struct / 单值 类型
func Scan(rows Rows, dst any) (rowsAffected int64, err error) {
	defer rows.Close()

	if rows == nil {
		return 0, errors.New("rows is nil")
	}

	// columns, err := rows.Columns()
	// if err != nil {
	// 	return 0, err
	// }

	//values := make([]any, len(columns))
	//columnTypes, _ := rows.ColumnTypes()

	switch out := dst.(type) {

	// ---- 单条 map ----
	case *map[string]any:
		if rows.Next() {
			err = sqlx.MapScan(rows, *out)
			if err != nil {
				return 0, err
			}
			rowsAffected = 1
		}
	case map[string]any:
		if rows.Next() {
			err = sqlx.MapScan(rows, out)
			if err != nil {
				return 0, err
			}
			rowsAffected = 1
		}

	// ---- 多条 map ----
	case *[]map[string]any:
		for rows.Next() {
			row := map[string]any{}
			err = sqlx.MapScan(rows, row)
			if err != nil {
				return 0, err
			}
			*out = append(*out, row)
			rowsAffected++
		}
	case *[]any:
		for rows.Next() {
			row, err := sqlx.SliceScan(rows)
			if err != nil {
				return 0, err
			}
			*out = append(*out, row)
			rowsAffected++
		}

	// ---- 基础类型 ----
	case *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64, *uintptr,
		*float32, *float64,
		*bool, *string, *time.Time,
		*sql.NullInt32, *sql.NullInt64, *sql.NullFloat64,
		*sql.NullBool, *sql.NullString, *sql.NullTime:
		for rows.Next() {
			if err = rows.Scan(out); err != nil {
				return rowsAffected, err
			}
			rowsAffected++
		}
	// ---- struct 或 struct 切片 ----
	default:
		rv := reflect.ValueOf(dst)
		if rv.Kind() != reflect.Ptr {
			return 0, errors.New("dst must be a pointer")
		}

		elem := rv.Elem()
		switch elem.Kind() {
		case reflect.Struct:
			if rows.Next() {
				err = scanIntoStruct(rows, elem)
				if err != nil {
					return rowsAffected, err
				}
				rowsAffected = 1
			}

		case reflect.Slice:
			sliceElemType := elem.Type().Elem()
			for rows.Next() {
				newElem := reflect.New(sliceElemType).Elem()
				if err = scanIntoStruct(rows, newElem); err != nil {
					return rowsAffected, err
				}
				elem.Set(reflect.Append(elem, newElem))
				rowsAffected++
			}

		default:
			return 0, errors.New("unsupported dst type")
		}
	}

	if err = rows.Err(); err != nil {
		return rowsAffected, err
	}
	return rowsAffected, nil
}

func PointerImplementFieldsI(dest reflect.Value) (fi FieldsI, ok bool, err error) {
	dest = reflect.Indirect(dest)
	destInterface := dest.Addr().Interface()
	fi, ok = destInterface.(FieldsI)
	if !ok {
		return nil, false, nil
	}
	implementsStruct := dest.Type().Implements(reflect.TypeOf((*FieldsI)(nil)).Elem())
	if implementsStruct {
		//实现FieldsI 必须是  func (m *KnowledgeNodeModel) Fields()  格式，而不是 func (m KnowledgeNodeModel) Fields()  格式(结构体实现接口，Fields赋值后无法传递给外部结构体变量)
		err := errors.Errorf("dest(%v) must pointer implement FieldsI,got struct  implement FieldsI", dest.String())
		return nil, false, err
	}
	return fi, true, nil
}

func scanIntoStruct(rows Rows, dest reflect.Value) error {
	if rows == nil {
		return nil
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fi, ok, err := PointerImplementFieldsI(dest)
	if err != nil {
		return err
	}
	if !ok { // 普通结构体扫描
		if err := sqlx.StructScan(rows, dest.Addr().Interface()); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return fmt.Errorf("sqlx.StructScan failed: %w", err)
		}
		return nil
	}

	// ---- 实现FieldsI接口扫描----

	fields := fi.Fields()
	addrs := make([]any, len(columns))
	canUseFields := false
	for i, col := range columns {
		if f, ok := fields.GetByName(col); ok {
			canUseFields = true
			addrs[i], err = f.GetValueRef()
			if err != nil {
				return err
			}
		}
	}
	if !canUseFields {
		err := errors.Errorf(`sql select name(%s) not match FieldsI.Fields() name(%s),please check sql select name or FieldsI.Fields() name`, strings.Join(columns, ","), strings.Join(fields.Names(), ","))
		return err
	}
	values, err := sqlx.SliceScan(rows)
	if err != nil {
		return err
	}
	for i := range values {
		if addrs[i] != nil {
			dRv := reflect.Indirect(reflect.ValueOf(addrs[i]))
			if values[i] == nil {
				continue
			}
			sRv := reflect.Indirect(reflect.ValueOf(values[i]))
			if sRv.CanConvert(dRv.Type()) {
				dRv.Set(sRv.Convert(dRv.Type()))
				continue
			}

			// ---- 手动转换类型 ----
			switch dRv.Kind() {
			case reflect.String:
				dRv.SetString(cast.ToString(values[i]))

			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				dRv.SetInt(cast.ToInt64(values[i]))

			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				dRv.SetUint(cast.ToUint64(values[i]))

			case reflect.Float32, reflect.Float64:
				dRv.SetFloat(cast.ToFloat64(values[i]))

			case reflect.Bool:
				dRv.SetBool(cast.ToBool(values[i]))

			default:
				// ---- ⏳ time.Time 特殊处理 ----
				if dRv.Type().String() == "time.Time" {
					v := cast.ToTime(values[i])
					dRv.Set(reflect.ValueOf(v))
				}
				err = errors.Errorf("StructScan failed: %v can not convert to %v", sRv.Type(), dRv.Type()) //todo 这部分如何处理
				panic(err)
			}
		}
	}
	return nil
}
