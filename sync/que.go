package sync

import (
	"sync"

	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
)

type Que[T any] interface {
	Push(data T)
	Pop() (data T, ok bool)
	Peek() (data T, ok bool)
	Delete(is func(v T) bool)
	HasData() <-chan struct{}
}

func NewQue[T any, RT bcts.ReadWriter[T]]() Que[T, RT] {
	return &que[T, RT]{signal: make(chan struct{})}
}

type que[T any, RT bcts.ReadWriter[T]] struct {
	signal chan struct{}
	data   []RT
	rwLock sync.RWMutex
	has    bool
}

func (s *que[T]) Push(data T) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data = append(s.data, data)
	if !s.has {
		s.has = true
		close(s.signal)
	}
}

func (s *que[T]) Pop() (data T, ok bool) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if len(s.data) == 0 {
		return
	}
	data, ok = s.data[0], true
	s.data = s.data[1:]
	if len(s.data) == 0 {
		s.has = false
		s.signal = make(chan struct{})
	} else {
		//log.Info("poped que", "el", data, "next", s.data[0])
		log.Trace("poped que")
	}
	return
}

func (s *que[T]) Delete(is func(v T) bool) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	for i, v := range s.data {
		if is(v) {
			s.data = append(s.data[:i], s.data[i+1:]...)
		}
	}
	if len(s.data) == 0 {
		s.has = false
		s.signal = make(chan struct{})
	}
}

func (s *que[T]) Peek() (data T, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	if len(s.data) == 0 {
		return
	}
	return s.data[0], true
}

func (s *que[T]) HasData() <-chan struct{} {
	return s.signal
}
