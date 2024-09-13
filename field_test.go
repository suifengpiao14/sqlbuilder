package sqlbuilder_test

import (
	"fmt"
	"testing"

	"github.com/suifengpiao14/sqlbuilder"
)

func TestCopy(t *testing.T) {
	f1 := &sqlbuilder.Field{
		Name: "f1",
		Schema: &sqlbuilder.Schema{
			Type: "f1",
		},
	}
	f2 := *f1
	f1.SetSelectColumns("f1")
	f2.Name = "f2"
	schema := *f1.Schema
	f2.Schema = &schema
	f2.Schema.Type = "f2"

	fmt.Println(f1.Name, f1.Schema.Type, f1.Select())
	fmt.Println(f2.Name, f2.Schema.Type, f2.Select())

}
