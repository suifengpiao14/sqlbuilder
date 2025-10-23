package sqlbuilder

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
)

// 针对实现了FieldsI 接口的模型，优先使用FieldIDBHandler 处理，能减少硬编码
type FieldIDBHandler func() *sql.DB

func NewFieldIDBHandler(getDB func() *sql.DB) Handler {
	return FieldIDBHandler(getDB)
}

func (h FieldIDBHandler) Transaction(fc func(tx Handler) error, opts ...*sql.TxOptions) (err error) {
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
func (h FieldIDBHandler) GetDialector() string {
	return DetectDriver(h())
}
func (h FieldIDBHandler) GetSqlDB() *sql.DB {
	return h()
}
func (h FieldIDBHandler) OriginalHandler() Handler {
	return h
}
func (h FieldIDBHandler) IsOriginalHandler() bool {
	return true
}

func (h FieldIDBHandler) Exec(sql string) (err error) {
	_, err = h().Exec(sql)
	if err != nil {
		return err
	}

	return nil
}
func (h FieldIDBHandler) ExecWithRowsAffected(sql string) (rowsAffected int64, err error) {
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

func (h FieldIDBHandler) Exists(sql string) (exists bool, err error) {
	result := make([]any, 0)
	ctx := context.Background()
	err = h.Query(ctx, sql, &result)
	if err != nil {
		return false, err
	}
	exists = len(result) > 0
	return exists, nil
}
func (h FieldIDBHandler) InsertWithLastId(sql string) (lastInsertId uint64, rowsAffected int64, err error) {

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
func (h FieldIDBHandler) First(_ context.Context, sqlstr string, result any) (exists bool, err error) {
	row := h().QueryRow(sqlstr)
	exists, err = ScanRow(row, result)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (h FieldIDBHandler) Query(_ context.Context, sqlstr string, result any) (err error) {
	rows, err := h().Query(sqlstr)
	if err != nil {
		return err
	}
	err = ScanRows(rows, result)
	if err != nil {
		return err
	}
	return nil
}

func (h FieldIDBHandler) Count(sql string) (count int64, err error) {
	err = h().QueryRow(sql).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (h FieldIDBHandler) GetDB() *sql.DB {
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
func (h _TxHandler) GetSqlDB() *sql.DB {
	return h.db
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
	row := h.tx.QueryRow(sqlstr)
	exists, err = ScanRow(row, result)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (h _TxHandler) Query(_ context.Context, sqlstr string, result any) (err error) {
	rows, err := h.tx.Query(sqlstr)
	if err != nil {
		return err
	}
	err = ScanRows(rows, result)
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

func ScanRows(rows *sql.Rows, dst any) (err error) {
	defer rows.Close()

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return errors.New("dst must be a pointer to slice")
	}
	sliceVal := dstVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return errors.New("dst must be a pointer to slice")
	}

	elemType := sliceVal.Type().Elem()
	// 检查是否实现 FieldsI
	fieldsIType := reflect.TypeOf((*FieldsI)(nil)).Elem()
	if !elemType.Implements(fieldsIType) && !(reflect.PointerTo(elemType).Implements(fieldsIType)) {
		return fmt.Errorf("slice element type %s does not implement FieldsI", elemType)
	}

	for rows.Next() {
		// 创建新实例
		var newElem reflect.Value
		if elemType.Kind() == reflect.Ptr {
			newElem = reflect.New(elemType.Elem())
		} else {
			newElem = reflect.New(elemType)
		}
		// 调用 Fields() 方法
		method := newElem.MethodByName("Fields")
		if !method.IsValid() {
			// 如果是非指针类型可能要尝试指针方法
			method = newElem.Elem().MethodByName("Fields")
		}
		if !method.IsValid() {
			return fmt.Errorf("type %s does not have Fields() method", elemType)
		}

		results := method.Call(nil)
		if len(results) != 1 {
			return fmt.Errorf("Fields() should return []FieldI")
		}
		fields := results[0].Interface().(Fields)

		// 收集字段地址
		addrs := fields.GetValueRefs()
		if err = rows.Scan(addrs...); err != nil {
			return err
		}
		// append 到 slice
		if elemType.Kind() != reflect.Ptr {
			sliceVal.Set(reflect.Append(sliceVal, newElem.Elem()))
		} else {
			sliceVal.Set(reflect.Append(sliceVal, newElem))
		}
	}
	err = rows.Err()
	return err
}

func ScanRow(row *sql.Row, dst any) (exists bool, err error) {
	if row == nil {
		return false, nil
	}

	// 获取类型信息
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return false, errors.New("dst 必须是非 nil 指针")
	}

	// 判断是否实现了 FieldsI 接口
	fieldsIType := reflect.TypeOf((*FieldsI)(nil)).Elem()
	if !rv.Type().Implements(fieldsIType) && !rv.Elem().Type().Implements(fieldsIType) {
		return false, errors.New("dst 未实现 FieldsI 接口")
	}

	// 取出实际的 FieldsI 实例
	var fi FieldsI
	if rv.Type().Implements(fieldsIType) {
		fi = rv.Interface().(FieldsI)
	} else {
		fi = rv.Elem().Interface().(FieldsI)
	}

	fs := fi.Fields()
	if len(fs) == 0 {
		return false, errors.New("Fields() 为空")
	}

	// 收集字段引用
	addrs := fs.GetValueRefs()
	// 执行 row.Scan
	err = row.Scan(addrs...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) { // 没有数据，不存在
			return false, nil
		}
		return false, err
	}

	return true, nil
}
