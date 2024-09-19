package sync

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/iidesho/gober/bcts"
)

type SLK interface {
	Add(key string, timeout time.Duration)
	Get(key string) (ok bool)
	Delete(key string)
}

func NewSLK() SLK {
	return &slk{
		rwLock: sync.RWMutex{},
		data:   make(map[bcts.TinyString]bcts.Time),
	}
}

type slk struct {
	data   map[bcts.TinyString]bcts.Time
	rwLock sync.RWMutex
}

func (s *slk) Add(key string, timeout time.Duration) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	s.data[bcts.TinyString(key)] = bcts.Time(time.Now().Add(timeout))
}

func (s *slk) Get(key string) (ok bool) {
	s.rwLock.RLock()
	timeout, ok := s.data[bcts.TinyString(key)]
	s.rwLock.RUnlock()
	if !ok {
		return
	}
	if timeout.Time().After(time.Now()) {
		return true
	}
	go s.Delete(key)
	//TODO: change it to use a watcher thread instead of this simple hack. This cuold leak mem by not cleaning old values that is not getting accessed
	return false //time.Now().After(timeout)
}

func (s *slk) Delete(key string) {
	s.rwLock.RLock()
	_, ok := s.data[bcts.TinyString(key)]
	s.rwLock.RUnlock()
	if !ok {
		return
	}
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	delete(s.data, bcts.TinyString(key))
}

func (s *slk) WriteBytes(w io.Writer) (err error) {
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
	return nil
}

func (s *slk) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid slice version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadMap(r, &s.data)
	if err != nil {
		return
	}
	return nil
}
