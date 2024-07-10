package sqlbuilder

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
