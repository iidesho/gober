package sync

import (
	"sync"

	"golang.org/x/exp/maps"
)

type Map[K comparable, V any] interface {
	Set(key K, data V)
	Get(key K) (data V, ok bool)
	GetOrInit(key K, init func() V) (data V, isNew bool)
	GetMap() map[K]V
	Delete(key K)
	CompareAndSwap(key K, n V, swap func(stored V) bool) (swapped bool)
}

func NewMap[K comparable, V any]() *sMap[K, V] {
	return &sMap[K, V]{
		rwLock: sync.RWMutex{},
		data:   make(map[K]V),
	}
}

type sMap[K comparable, V any] struct {
	data   map[K]V
	rwLock sync.RWMutex
}

func (s *sMap[K, V]) Set(key K, data V) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data[key] = data
}

func (s *sMap[K, V]) Get(key K) (data V, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	data, ok = s.data[key]
	return
}

func (s *sMap[K, V]) GetOrInit(key K, init func() V) (V, bool) {
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

func (s *sMap[K, V]) GetMap() (data map[K]V) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return maps.Clone(s.data)
}

func (s *sMap[K, V]) Delete(key K) {
	_, ok := s.Get(key)
	if !ok {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	delete(s.data, key)
	return
}

func (s *sMap[K, V]) CompareAndSwap(
	key K,
	val V,
	swap func(stored V) bool,
) (swapped bool) {
	stored, ok := s.Get(key)
	if ok && !swap(stored) {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	stored, ok = s.data[key]
	if !ok || swap(stored) {
		s.data[key] = val
		return true
	}
	return
}
