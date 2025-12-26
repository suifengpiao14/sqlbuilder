package sqlbuilder_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlbuilder"
)

func TestIndirectHandler(t *testing.T) {
	gormHandler := sqlbuilder.NewGormHandler(nil)
	oHandler := sqlbuilder.WithCacheSingleflightHandler(gormHandler, true, true)
	iHandler := oHandler.OriginalHandler()
	require.IsType(t, gormHandler, iHandler)
}

type Entity struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

type Field struct {
	Value any
}

type Fields []Field

type FieldsI interface {
	Fields() Fields
}

var cp = Entity{}

func (e Entity) Fields() Fields {
	cp = e
	return Fields{
		{Value: &cp.Name},
	}
}

func TestEntity(t *testing.T) {
	e := Entity{}
	ref := &e

	fi := ref.Fields()
	for _, f := range fi {
		switch v := f.Value.(type) {
		case *string:
			*v = "abc"

		}
	}

	// 步骤2：调用通用反射函数，从Name字段指针推导副本e的地址
	structPtr, err := sqlbuilder.GetStructPtrFromFieldPtr(
		reflect.ValueOf(fi[0].Value), // 副本e的Name字段指针
		reflect.TypeOf(Entity{}),     // Entity结构体类型
	)
	require.NoError(t, err) // 确保无错误
	// 步骤3：类型断言为*Entity，拿到副本e的指针
	copyE := structPtr.(*Entity)
	require.Equal(t, "abc", copyE.Name) // 测试不通过

}
