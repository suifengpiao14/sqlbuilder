package sqlbuilder

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"
	"unsafe"

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

// ---- 实现FieldsI接口扫描---- isStructImplements 标记是否为结构体实现FieldsI接口而非指针实现，这种情况需要寻址 找到真正被赋值的副本
func IsStructImplementFieldsI(dest reflect.Value) (fi FieldsI, ok bool, isStructImplements bool) {
	dest = reflect.Indirect(dest)
	switch dest.Kind() {
	case reflect.Ptr:
		dest = dest.Elem()
	case reflect.Array, reflect.Slice: // 数组切片 需要寻址到元素类型，否则无法赋值给外部变量
		dest = reflect.New(dest.Type().Elem()).Elem()
	}
	if dest.Kind() != reflect.Struct { // 非结构体 直接返回
		return nil, false, false
	}

	destInterface := dest.Addr().Interface()
	fi, ok = destInterface.(FieldsI)
	if !ok {
		return nil, false, false
	}
	//isStructImplements = dest.Type().Implements(reflect.TypeOf((*FieldsI)(nil)).Elem())
	isStructImplements = dest.Type().Implements(reflect.TypeFor[FieldsI]())
	// if implementsStruct {
	// 	//实现FieldsI 必须是  func (m *KnowledgeNodeModel) Fields()  格式，而不是 func (m KnowledgeNodeModel) Fields()  格式(结构体实现接口，Fields赋值后无法传递给外部结构体变量)
	// 	err := errors.Errorf("dest(%v) must pointer implement FieldsI,got struct  implement FieldsI", dest.String())
	// 	return nil, false, err
	// }
	return fi, true, isStructImplements
}

func scanIntoStruct(rows Rows, dest reflect.Value) error {
	if rows == nil {
		return nil
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fi, ok, isStructImplements := IsStructImplementFieldsI(dest)
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
	//canUseFields := false
	for i, col := range columns {
		if f, ok := fields.GetByName(col); ok {
			//canUseFields = true
			addrs[i], err = f.GetValueRef()
			if err != nil {
				return err
			}
		}
	}
	// if !canUseFields {//缺失的字段读取到any即可 无需报错 2025-12-26 10:41
	// 	err := errors.Errorf(`sql select name(%s) not match FieldsI.Fields() name(%s),please check sql select name or FieldsI.Fields() name`, strings.Join(columns, ","), strings.Join(fields.Names(), ","))
	// 	return err
	// }
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
			// if sRv.CanConvert(dRv.Type()) {
			// 	dRv.Set(sRv.Convert(dRv.Type()))
			// 	continue
			// }

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
	if isStructImplements {
		// 结构体实现FieldsI 则需要寻址
		for _, val := range addrs {
			if val != nil {
				fieldVal := reflect.ValueOf(val)
				if fieldVal.Kind() == reflect.Ptr { // 必须找到指针类型，否则无法寻址
					hasValueDst, err := GetStructPtrFromFieldPtr(fieldVal, dest.Type())
					if err != nil {
						return nil
					}
					if hasValueDst != nil { // 寻址成功,直接赋值即可
						dest.Set(reflect.Indirect(reflect.ValueOf(hasValueDst)))
						return nil // 寻址成功，则返回
					}
				}
			}
		}
	}

	return nil
}

// 通用反射函数：从字段指针推导结构体实例指针
// 参数：
//
//	fieldPtr: 结构体字段的指针（如&e.Name）
//	structType: 目标结构体的类型（如reflect.TypeOf(Entity{})）
//
// 返回：结构体实例的指针（如*Entity）
func GetStructPtrFromFieldPtr(fieldVal reflect.Value, structType reflect.Type) (any, error) {
	// 1. 基础校验
	if structType.Kind() != reflect.Struct {
		return nil, errors.New("structType must be a struct type")
	}

	//	fieldVal := reflect.ValueOf(fieldPtr)
	if fieldVal.Kind() != reflect.Ptr {
		return nil, errors.New("fieldPtr must be a pointer to a struct field")
	}
	fieldElemType := fieldVal.Type().Elem()

	// 2. 获取字段指针的底层地址（直接通过unsafe.Pointer转换，缩短链）
	fieldPointer := fieldVal.Pointer() // 返回uintptr类型的字段地址
	if fieldPointer == 0 {
		return nil, errors.New("fieldPtr is a nil pointer")
	}

	// 3. 遍历结构体字段，自动匹配目标字段
	var matchedOffset uintptr
	matched := false

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if field.Type != fieldElemType {
			continue
		}

		// 核心优化：遵循Go unsafe规范，直接链式转换（无中间uintptr变量）
		// 步骤1：计算候选结构体地址 → 直接转换为unsafe.Pointer（不保存uintptr）
		// 合规使用：从字段地址推导结构体地址，符合Go unsafe规范
		// nolint:unsafeptr
		candidateStructPtrUnsafe := unsafe.Pointer(fieldPointer - field.Offset)

		// 步骤2：通过reflect.NewAt创建结构体指针（合规操作）
		candidateStructVal := reflect.NewAt(structType, candidateStructPtrUnsafe)
		// 步骤3：获取候选结构体的该字段指针，验证地址匹配
		candidateFieldPtr := candidateStructVal.Elem().Field(i).Addr().Interface()

		// 地址验证：确认是目标字段
		if reflect.ValueOf(candidateFieldPtr).Pointer() == fieldPointer {
			matchedOffset = field.Offset
			matched = true
			break
		}
	}

	if !matched {
		return nil, fmt.Errorf("no matching field of type %s found in struct %s", fieldElemType, structType.Name())
	}

	// 4. 最终转换：同样遵循链式转换，抑制lint提示
	// nolint:unsafeptr // 合规使用：结构体地址推导完成，转换为目标指针
	structPtrUnsafe := unsafe.Pointer(fieldPointer - matchedOffset)
	structPtr := reflect.NewAt(structType, structPtrUnsafe).Interface()

	return structPtr, nil
}
