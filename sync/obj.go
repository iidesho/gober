package sync

import (
	"sync/atomic"
)

type Obj[T any] interface {
	Set(data T)
	Get() (data T)
	Swap(inn T) (out T)
	//Comp(data T) bool
}

func NewObj[T any]() Obj[T] {
	return &sObj[T]{
		p: &atomic.Pointer[T]{},
	}
}

type sObj[T any] struct {
	p *atomic.Pointer[T]
}

func (s *sObj[T]) Set(data T) {
	s.p.Store(&data)
}

func (s *sObj[T]) Get() (out T) {
	p := s.p.Load()
	if p == nil {
		return
	}
	return *p
}

func (s *sObj[T]) Swap(inn T) (out T) {
	p := s.p.Swap(&inn)
	if p == nil {
		return
	}
	return *p
}

/*
func (s *sObj[T]) Comp(data T) bool {
	return s.Get() == data
}
*/
