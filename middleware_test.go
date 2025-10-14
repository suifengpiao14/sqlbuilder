package sqlbuilder_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlbuilder"
)

func TestValueFnFnIncrease(t *testing.T) {
	val, err := sqlbuilder.ValueFnFnIncrease(1, nil)
	require.NoError(t, err)
	fmt.Println(val)
}
