package sqlbuilder_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlbuilder"
)

func TestIndirectHandler(t *testing.T) {
	gormHandler := sqlbuilder.NewGormHandler(nil)
	oHandler := sqlbuilder.WithCacheSingleflightHandler(gormHandler, true, true)
	iHandler := oHandler.IndirectHandler()
	require.IsType(t, gormHandler, iHandler)
}
