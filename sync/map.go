package sync

import (
	"sync"

	"golang.org/x/exp/maps"
)

type Map[T any] interface {
	Set(key string, data T)
	Get(key string) (data T, ok bool)
	GetOrInit(key string, init func() T) (data T, isNew bool)
	GetMap() map[string]T
	Delete(key string)
	CompareAndSwap(key string, n T, swap func(stored T) bool) (swapped bool)
}

func NewMap[T any]() Map[T] {
	return &sMap[T]{
		rwLock: sync.RWMutex{},
		data:   make(map[string]T),
	}
}

type sMap[T any] struct {
	data   map[string]T
	rwLock sync.RWMutex
}

func (s *sMap[T]) Set(key string, data T) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data[key] = data
}

func (s *sMap[T]) Get(key string) (data T, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	data, ok = s.data[key]
	return
}

func (s *sMap[T]) GetOrInit(key string, init func() T) (T, bool) {
	//This somehow breaks the consesus algorighm, so even though we don't want to take a write lock here, we do it so that we don't need to debug that now.
	/*
		data, ok := s.Get(key)
		if ok {
			return data, ok
		}
	*/
	//We don't really want to take a write lock, so since that is expensive anyways we verify with the read lock first and
	// if not then we re verify that nothing has happened before we re aquired a write lock
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	data, ok := s.data[key]
	if !ok {
		data = init()
		s.data[key] = data
	}
	return data, !ok
}

func (s *sMap[T]) GetMap() (data map[string]T) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return maps.Clone(s.data)
}

func (s *sMap[T]) Delete(key string) {
	_, ok := s.Get(key)
	if !ok {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	delete(s.data, key)
	return
}

func (s *sMap[T]) CompareAndSwap(key string, n T, swap func(stored T) bool) (swapped bool) {
	stored, ok := s.Get(key)
	if ok && !swap(stored) {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	stored, ok = s.data[key]
	if !ok || swap(stored) {
		s.data[key] = n
		return true
	}
	return
}
