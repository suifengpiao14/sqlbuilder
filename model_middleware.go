package sqlbuilder

type modelMiddlewarePool struct {
	index       int
	middlewares ModelMiddlewareFns
}

func (p modelMiddlewarePool) append(fns ...ModelMiddleware) modelMiddlewarePool {
	p.middlewares = p.middlewares.append(fns...)
	return p
}

type ModelMiddleware func(fsRef *Fields) (err error)
type ModelMiddlewareFns []ModelMiddleware

func (rms ModelMiddlewareFns) append(fns ...ModelMiddleware) ModelMiddlewareFns {
	rms = append(rms, fns...)
	return rms
}

// Next 传递控制权给下一个中间件
// 实现逻辑：索引+1并执行下一个中间件
func (pipe modelMiddlewarePool) Next(fs *Fields) (err error) {
	pipe.index++
	if pipe.index < len(pipe.middlewares) {
		middleware := pipe.middlewares[pipe.index]
		if middleware == nil {
			return pipe.Next(fs) //如果当前场景不匹配或者fn为空，则继续执行下一个fn
		}
		err = middleware(fs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pipe modelMiddlewarePool) run(table TableConfig, fs Fields) (err error) {
	pipe.index = -1
	cpfs := fs.Copy().SetTable(table) //使用副本
	err = pipe.Next(&cpfs)
	if err != nil {
		return err
	}
	return nil
}
