package mergedcontext

import (
	"context"
	"github.com/pkg/errors"
	"time"
)

func MergeContexts(ctx1, ctx2 context.Context) (ctxOut context.Context, cancel context.CancelFunc) {
	done := make(chan struct{}, 0)
	ctxOut = &mergedContexts{
		ctx1: ctx1,
		ctx2: ctx2,
		done: done,
	}
	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case done <- <-ctx1.Done():
			case done <- <-ctx2.Done():
			}
		}
	}()

	return
}

type mergedContexts struct {
	ctx1 context.Context
	ctx2 context.Context
	done <-chan struct{}
}

func (c *mergedContexts) Deadline() (deadline time.Time, ok bool) {
	d1, ok1 := c.ctx1.Deadline()
	d2, ok2 := c.ctx2.Deadline()
	if !ok2 {
		return d1, ok1
	}
	if !ok1 {
		return d2, ok2
	}
	if d1.Before(d2) {
		return d1, ok1
	}
	return d2, ok2
}

func (c *mergedContexts) Done() <-chan struct{} {
	return c.done
}

func (c *mergedContexts) Err() error {
	if c.ctx2.Err() == nil {
		return c.ctx1.Err()
	}
	if c.ctx1.Err() == nil {
		return c.ctx2.Err()
	}
	return errors.Wrap(c.ctx1.Err(), c.ctx2.Err().Error())
}

func (c *mergedContexts) Value(key any) (val any) {
	val = c.ctx1.Value(key)
	if val != nil {
		return
	}
	return c.ctx2.Value(key)
}
