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

type ModelMiddleware func(ctx *ModelMiddlewareContext, fsRef *Fields) (err error)
type ModelMiddlewares []ModelMiddleware

func (rms ModelMiddlewares) append(fns ...ModelMiddleware) ModelMiddlewares {
	rms = append(rms, fns...)
	return rms
}

// Next 传递控制权给下一个中间件
// 实现逻辑：索引+1并执行下一个中间件
func (ctx *ModelMiddlewareContext) Next(fs *Fields) (err error) {
	ctx.index++
	if ctx.index < len(ctx.middlewares) {
		middleware := ctx.middlewares[ctx.index]
		if middleware == nil {
			return ctx.Next(fs) //如果当前场景不匹配或者fn为空，则继续执行下一个fn
		}
		err = middleware(ctx, fs)
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
