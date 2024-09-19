package sync

import (
	"fmt"
	"io"
	"sync"

	"github.com/iidesho/gober/bcts"
)

/*
type Stack[T any, RT bcts.ReadWriter[T]] interface {
	Push(data RT)
	Pop() (data RT, ok bool)
	Peek() (data RT, ok bool)
	WriteBytes(w *bufio.Writer) (err error)
	ReadBytes(r io.Reader) (err error)
}
*/

func NewStack[T any, RT bcts.ReadWriter[T]]() *Stack[T, RT] {
	return &Stack[T, RT]{}
}

type Stack[T any, RT bcts.ReadWriter[T]] struct {
	data   []RT
	rwLock sync.RWMutex
}

func (s *Stack[T, RT]) Push(data RT) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data = append(s.data, data)
}

func (s *Stack[T, RT]) Pop() (data RT, ok bool) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	if len(s.data) == 0 {
		return
	}
	data, ok = s.data[len(s.data)-1], true
	s.data = s.data[:len(s.data)-1]
	return
}

func (s *Stack[T, RT]) Peek() (data RT, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	if len(s.data) == 0 {
		return
	}
	return s.data[len(s.data)-1], true
}

func (s *Stack[T, RT]) WriteBytes(w io.Writer) (err error) {
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

func (s *Stack[T, RT]) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid slice version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadSlice(r, &s.data)
	if err != nil {
		return
	}
	return nil
}
