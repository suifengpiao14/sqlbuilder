package sqlbuilder

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/spf13/cast"
)

type ApiDocument struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Path        string `json:"path"`
	Method      string `json:"method"`
	ContentType string `json:"contentType"`
	//todo 融合apidml 项目部分内容
}

type Api struct {
	Input string `json:"input"`
}

func (api *Api) Route() (method string, path string) {
	return
}

func (api *Api) Fields() (fields Fields) {
	return
}

func (api *Api) Doc() (doc ApiDocument) {
	return
}

type DocRequestArg struct {
	Name        string `json:"name"`
	Required    bool   `json:"required,string"`
	AllowEmpty  bool   `json:"allowEmpty,string"`
	Title       string `json:"title"`
	Type        string `json:"type"`
	Format      string `json:"format"`
	Default     string `json:"default"`
	Description string `json:"description"`
	Example     string `json:"example"`
	Enums       Enums  `json:"enums"`
	RegExp      string `json:"regExp"`
}

type DocRequestArgs []DocRequestArg

func (args DocRequestArgs) Makedown() string {
	var w bytes.Buffer
	w.WriteString(`|名称|类型|必填|标题|描述|`)
	w.WriteString("\n")
	w.WriteString(`|:--|:--|:--|:--|:--|:--|`)
	w.WriteString("\n")
	for _, arg := range args {
		description := arg.Description
		if len(arg.Enums) > 0 {
			description = fmt.Sprintf("%s(%s)", description, arg.Enums.String())
		}
		row := fmt.Sprintf(`|%s|%s|%s|%s|%s|`,
			arg.Name,
			arg.Type,
			cast.ToString(arg.Required),
			arg.Title,
			description,
		)
		w.WriteString(row)
		w.WriteString("\n")
	}
	return w.String()
}

func (args DocRequestArgs) JsonExample(pretty bool) string {
	m := map[string]any{}
	for _, arg := range args {
		m[arg.Name] = arg.Example
		if m[arg.Name] == "" {
			m[arg.Name] = arg.Default
		}
	}
	var w bytes.Buffer
	marshal := json.NewEncoder(&w)
	marshal.SetIndent("", " ")
	marshal.Encode(m)
	return w.String()

}
