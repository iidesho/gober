package sync

import (
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/iidesho/gober/bcts"
)

type SliceBCTS[T any, RT bcts.ReadWriter[T]] interface {
	Add(data T)
	Set(i int, data T)
	Get(i int) (data T, ok bool)
	Len() int
	Contains(val T) int
	SliceBCTS() []T
	Delete(i int) T
	Clear()
}

func NewSliceBCTS[T any, RT bcts.ReadWriter[T]]() *sliceBCTS[T, RT] {
	return &sliceBCTS[T, RT]{}
}

type sliceBCTS[T any, RT bcts.ReadWriter[T]] struct {
	data   []RT
	dloc   []*sync.RWMutex
	rwLock sync.RWMutex
}

func (s *sliceBCTS[T, RT]) Add(data T) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data = append(s.data, &data)
	s.dloc = append(s.dloc, &sync.RWMutex{})
}

func (s *sliceBCTS[T, RT]) Set(i int, data T) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	s.dloc[i].Lock()
	defer s.dloc[i].Unlock()
	s.data[i] = &data
}

func (s *sliceBCTS[T, RT]) Get(i int) (data T, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	s.dloc[i].RLock()
	defer s.dloc[i].RUnlock()
	if i < 0 || i >= len(s.data) {
		return
	}
	data = *s.data[i]
	return
}

func (s *sliceBCTS[T, RT]) Len() int {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return len(s.data)
}

func (s *sliceBCTS[T, RT]) Contains(val T) int {
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

func (s *sliceBCTS[T, RT]) SliceBCTS() (data []T) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	// copy(data, s.data)
	data = make([]T, len(s.data))
	for i := range s.data {
		// Should consider impact of  aquiering and releasing the read lock of each element
		s.dloc[i].RLock()
		data[i] = *s.data[i]
		s.dloc[i].RUnlock()
	}
	return
}

func (s *sliceBCTS[T, RT]) Delete(i int) (d T) {
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

func (s *sliceBCTS[T, RT]) Clear() {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.dloc = []*sync.RWMutex{}
	s.data = []RT{}
}

func (s *sliceBCTS[T, RT]) WriteBytes(w io.Writer) (err error) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	err = bcts.WriteUInt8(w, uint8(0)) // Version
	if err != nil {
		return
	}
	err = bcts.WriteSlice(w, s.data)
	if err != nil {
		return
	}
	return nil
}

func (s *sliceBCTS[T, RT]) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid sliceBCTS version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadSlice(r, &s.data)
	if err != nil {
		return
	}
	return nil
}
