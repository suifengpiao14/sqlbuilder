package sqlbuilder

// Column 供中间件插入数据时,定制化值类型 如 插件为了运算方便,值声明为float64 类型,而数据库需要string类型此时需要通过匿名函数修改值
type Column struct {
	Name  string `json:"name"`
	Value func(in any) any
}
