package sync

import (
	"bufio"
	"fmt"
	"io"
	"sync"

	"github.com/iidesho/gober/bcts"
	"golang.org/x/exp/maps"
)

/*
type Map[T bcts.Writer, T bcts.ReadWriter[T]] interface {
	Set(key bcts.TinyString, data T)
	Get(key bcts.TinyString) (data T, ok bool)
	GetOrInit(key bcts.TinyString, init func() T) (data T, isNew bool)
	GetMap() map[bcts.TinyString]T
	Delete(key bcts.TinyString)
	CompareAndSwap(key bcts.TinyString, n T, swap func(stored T) bool) (swapped bool)
	WriteBytes(w *bufio.Writer) (err error)
}
*/

func NewMap[BT any, T bcts.ReadWriter[BT]]() *Map[BT, T] {
	return &Map[BT, T]{
		rwLock: sync.RWMutex{},
		data:   make(map[bcts.TinyString]T),
	}
}

func MapFromReader[BT any, T bcts.ReadWriter[BT]](r io.Reader) (*Map[BT, T], error) {
	m := Map[BT, T]{
		rwLock: sync.RWMutex{},
		data:   make(map[bcts.TinyString]T),
	}
	err := m.ReadBytes(r)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

type Map[BT any, T bcts.ReadWriter[BT]] struct {
	data   map[bcts.TinyString]T
	rwLock sync.RWMutex
}

func (s *Map[BT, T]) Set(key bcts.TinyString, data T) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data[key] = data
}

func (s *Map[BT, T]) Get(key bcts.TinyString) (data T, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	data, ok = s.data[key]
	return
}

func (s *Map[BT, T]) GetOrInit(key bcts.TinyString, init func() T) (T, bool) {
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

func (s *Map[BT, T]) GetMap() (data map[bcts.TinyString]T) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return maps.Clone(s.data)
}

func (s *Map[BT, T]) Delete(key bcts.TinyString) {
	_, ok := s.Get(key)
	if !ok {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	delete(s.data, key)
	return
}

func (s *Map[BT, T]) CompareAndSwap(
	key bcts.TinyString,
	n T,
	swap func(stored T) bool,
) (swapped bool) {
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

func (s *Map[BT, T]) WriteBytes(w *bufio.Writer) (err error) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	err = bcts.WriteUInt8(w, uint8(0)) //Version
	if err != nil {
		return
	}
	err = bcts.WriteMap(w, s.data)
	if err != nil {
		return
	}
	return w.Flush()
}

func (s *Map[BT, T]) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid slice version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	//err = bcts.ReadMap[bcts.TinyString, *bcts.TinyString, T, T](r, &s.data)
	if err != nil {
		return
	}
	return nil
}
