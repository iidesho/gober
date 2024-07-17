package sync

import (
	"reflect"
	"sync"
)

// Not sure i want to use comparable here
type Slice[T any] interface {
	Add(data T)
	Set(i int, data T)
	Get(i int) (data T, ok bool)
	Contains(val T) int
	Slice() []T
	Delete(i int) T
}

func NewSlice[T any]() Slice[T] {
	return &slice[T]{}
}

type slice[T any] struct {
	data   []T
	dloc   []*sync.RWMutex
	rwLock sync.RWMutex
}

func (s *slice[T]) Add(data T) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data = append(s.data, data)
	s.dloc = append(s.dloc, &sync.RWMutex{})
}

func (s *slice[T]) Set(i int, data T) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	s.dloc[i].Lock()
	defer s.dloc[i].Unlock()
	s.data[i] = data
}

func (s *slice[T]) Get(i int) (data T, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	s.dloc[i].RLock()
	defer s.dloc[i].RUnlock()
	if i < 0 || i >= len(s.data) {
		return
	}
	data = s.data[i]
	return
}

func (s *slice[T]) Contains(val T) int {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	/* For simplicity ill just use deep equal for now
	cval, ok := val.(comparable)
	for i, v := range s.data {
		if v == val {
			return i
		}
	}
	*/
	for i, v := range s.data {
		if reflect.DeepEqual(val, v) {
			return i
		}
	}
	return -1
}

func (s *slice[T]) Slice() (data []T) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	copy(data, s.data)
	return
}

func (s *slice[T]) Delete(i int) (d T) {
	var ok bool
	d, ok = s.Get(i)
	if !ok {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data = append(s.data[:i], s.data[i:]...)
	s.dloc = s.dloc[:len(s.dloc)-1] //All locks should be unlocked at this point
	return
}
