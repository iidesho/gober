package sync

import (
	"reflect"
	"sync"

	"github.com/iidesho/gober/itr"
)

type Slice[T any] interface {
	Add(data T)
	AddUnique(data T, eq func(v1, v2 T) bool) bool
	Set(i int, data T)
	Get(i int) (data T, ok bool)
	Len() int
	Contains(val T) int
	Slice() []T
	Delete(i int) T
	DeleteWhere(where func(v T) bool)
	Clear()
	ReadItr() itr.Iterator[T]
	WriteItr() itr.Iterator[*T]
}

func NewSlice[T any]() *slice[T] {
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

func (s *slice[T]) AddUnique(data T, eq func(v1, v2 T) bool) bool {
	s.rwLock.RLock()
	for _, v := range s.data {
		if eq(data, v) {
			s.rwLock.RUnlock()
			return false
		}
	}
	s.rwLock.RUnlock()
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	for _, v := range s.data {
		if eq(data, v) {
			return false
		}
	}
	s.data = append(s.data, data)
	s.dloc = append(s.dloc, &sync.RWMutex{})
	return true
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

func (s *slice[T]) Len() int {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return len(s.data)
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
	// copy(data, s.data)
	data = make([]T, len(s.data))
	for i := range s.data {
		// Should consider impact of  aquiering and releasing the read lock of each element
		s.dloc[i].RLock()
		data[i] = s.data[i]
		s.dloc[i].RUnlock()
	}
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
	s.dloc = s.dloc[:len(s.dloc)-1] // All locks should be unlocked at this point
	return
}

func (s *slice[T]) DeleteWhere(where func(v T) bool) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data = itr.NewIterator(s.data).
		Filter(func(s T) bool {
			return !where(s)
		}).Collect()
	// s.data = append(s.data[:i], s.data[i:]...)
	s.dloc = s.dloc[:len(s.data)] // All locks should be unlocked at this point
}

func (s *slice[T]) ReadItr() itr.Iterator[T] {
	s.rwLock.RLock()
	return func(yield func(T) bool) {
		defer s.rwLock.RUnlock()
		for _, i := range s.data {
			if !yield(i) {
				break
			}
		}
	}
}

func (s *slice[T]) WriteItr() itr.Iterator[*T] {
	s.rwLock.Lock()
	return func(yield func(*T) bool) {
		defer s.rwLock.Unlock()
		for i := range s.data {
			if !yield(&s.data[i]) {
				break
			}
		}
	}
}

func (s *slice[T]) Clear() {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.dloc = []*sync.RWMutex{}
	s.data = []T{}
}
