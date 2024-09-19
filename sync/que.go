package sync

import (
	"fmt"
	"io"
	"sync"

	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
)

/*
type Que[T any, RT bcts.ReadWriter[T]] interface {
	Push(data RT)
	Pop() (data RT, ok bool)
	Peek() (data RT, ok bool)
	Delete(is func(v T) bool)
	HasData() <-chan struct{}
	bcts.Writer
}
*/

func NewQue[BT any, T bcts.ReadWriter[BT]]() *Que[BT, T] {
	return &Que[BT, T]{signal: make(chan struct{})}
}

func QueFromReader[BT any, T bcts.ReadWriter[BT]](r io.Reader) (*Que[BT, T], error) {
	q := Que[BT, T]{signal: make(chan struct{})}
	err := q.ReadBytes(r)
	if err != nil {
		return nil, err
	}
	return &q, nil
}

type Que[T any, RT bcts.ReadWriter[T]] struct {
	signal chan struct{}
	data   []RT
	rwLock sync.RWMutex
	has    bool
}

func (s *Que[BT, T]) Push(data T) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data = append(s.data, data)
	if !s.has {
		s.has = true
		close(s.signal)
	}
}

func (s *Que[BT, T]) Pop() (data T, ok bool) {
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

func (s *Que[BT, T]) Delete(is func(v T) bool) {
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

func (s *Que[BT, T]) Peek() (data T, ok bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	if len(s.data) == 0 {
		return
	}
	return s.data[0], true
}

func (s *Que[BT, T]) HasData() <-chan struct{} {
	return s.signal
}

func (s *Que[BT, T]) WriteBytes(w io.Writer) (err error) {
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

func (s *Que[BT, T]) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid que version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadSlice(r, &s.data)
	if err != nil {
		return
	}
	return nil
}
