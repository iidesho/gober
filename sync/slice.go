package sync

import (
	"fmt"
	"io"
	"reflect"
	"sync"

	"github.com/iidesho/gober/bcts"
)

/*
type Slice[T any, RT bcts.ReadWriter[T]] interface {
	Add(data RT)
	Set(i int, data RT)
	Get(i int) (data RT, ok bool)
	Contains(val T) int
	Slice() []RT
	Delete(i int) RT
}
*/

func NewSlice[T bcts.ReadWriter[any]]() *slice[T] {
	return &slice[T]{}
}

type slice[T bcts.ReadWriter[any]] struct {
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

func (s *slice[T]) WriteBytes(w io.Writer) (err error) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	err = bcts.WriteUInt8(w, uint8(0)) //Version
	if err != nil {
		return
	}
	err = bcts.WriteSlice(w, s.data)
	if err != nil {
		return
	}
	return nil
}

func (s *slice[T]) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid slice version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	//err = bcts.ReadSlice(r, &s.data)
	if err != nil {
		return
	}
	return nil
}
