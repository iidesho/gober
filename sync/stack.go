package sync

import (
	"sync"
)

type Stack[T any] interface {
	Push(data T)
	Pop() (data T, ok bool)
	Peek() (data T, ok bool)
}

func NewStack[T any]() Stack[T] {
	return &stack[T]{}
}

type stack[T any] struct {
	data   []T
	rwLock sync.Mutex
}

func (s *stack[T]) Push(data T) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data = append(s.data, data)
}

func (s *stack[T]) Pop() (data T, ok bool) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if len(s.data) == 0 {
		return
	}
	data, ok = s.data[len(s.data)-1], true
	s.data = s.data[:len(s.data)-1]
	return
}

func (s *stack[T]) Peek() (data T, ok bool) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if len(s.data) == 0 {
		return
	}
	return s.data[len(s.data)-1], true
}
