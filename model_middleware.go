package sqlbuilder

import "context"

type ModelMiddlewareContext struct {
	Context     context.Context
	index       int
	middlewares ModelMiddlewares
}

func (ctx ModelMiddlewareContext) append(fns ...ModelMiddleware) ModelMiddlewareContext {
	ctx.middlewares = ctx.middlewares.append(fns...)
	return ctx
}

type ModelMiddleware struct {
	Name        string
	Description string
	Fn          func(ctx *ModelMiddlewareContext, fs *Fields) error
}
type ModelMiddlewares []ModelMiddleware

func (rms ModelMiddlewares) append(middlewars ...ModelMiddleware) ModelMiddlewares {
	middlewarArr := make([]ModelMiddleware, 0)
	m := make(map[string]struct{})
	for _, middleware := range rms {
		if _, ok := m[middleware.Name]; !ok {
			middlewarArr = append(middlewarArr, middleware)
			m[middleware.Name] = struct{}{}
		}
	}
	for _, middleware := range middlewars {
		if _, ok := m[middleware.Name]; !ok {
			middlewarArr = append(middlewarArr, middleware)
			m[middleware.Name] = struct{}{}
		}

	}
	return middlewarArr
}

// Next 传递控制权给下一个中间件
// 实现逻辑：索引+1并执行下一个中间件
func (ctx *ModelMiddlewareContext) Next(fs *Fields) (err error) {
	ctx.index++
	if ctx.index < len(ctx.middlewares) {
		middleware := ctx.middlewares[ctx.index]
		if middleware.Fn == nil {
			return ctx.Next(fs) //如果当前场景不匹配或者fn为空，则继续执行下一个fn
		}
		err = middleware.Fn(ctx, fs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ctx *ModelMiddlewareContext) run(table TableConfig, fs Fields) (err error) {
	ctx.index = -1
	cpfs := fs.Copy().SetTable(table) //使用副本
	err = ctx.Next(&cpfs)
	if err != nil {
		return err
	}
	return nil
}
