package sqlbuilder

import (
	"fmt"

	"github.com/doug-martin/goqu/v9"
)

// IncreaseMiddleware 字段值递增中间件
var IncreaseMiddleware MiddlewareFn = func(f *Field, fs ...*Field) {
	f.ValueFns.AppendIfNotFirst(func(inputValue any) (any, error) {
		val := fmt.Sprintf("`%s`+1", f.DBName())
		return goqu.L(val), nil
	})
}
