package syncmap

import (
	"golang.org/x/exp/maps"
	"sync"
)

type SyncMap[T any] interface {
	Set(key string, data T)
	Get(key string) (data T, ok bool)
	GetMap() map[string]T
}

func New[T any]() SyncMap[T] {
	return &syncMap[T]{
		rwLock: sync.RWMutex{},
		data:   make(map[string]T),
	}
}

type syncMap[T any] struct {
	rwLock sync.RWMutex
	data   map[string]T
}

func (s *syncMap[T]) Set(key string, data T) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data[key] = data
}

func (s *syncMap[T]) Get(key string) (data T, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	data, ok = s.data[key]
	return
}

func (s *syncMap[T]) GetMap() (data map[string]T) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return maps.Clone(s.data)
}
