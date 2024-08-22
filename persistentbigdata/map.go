package persistentbigmap

import "sync"

type mp[T any] struct {
	data map[string]T
	l    sync.Mutex
}

type Map[T any] interface {
	Store(id string, data T)
	Load(id string) (data T, ok bool)
	Delete(id string)
	Range(f func(k string, v T) bool)
}

func New[T any]() Map[T] {
	return &mp[T]{
		l:    sync.Mutex{},
		data: make(map[string]T),
	}
}

func (m *mp[T]) Store(id string, data T) {
	m.l.Lock()
	defer m.l.Unlock()
	m.data[id] = data
}

func (m *mp[T]) Load(id string) (data T, ok bool) {
	m.l.Lock()
	defer m.l.Unlock()
	data, ok = m.data[id]
	return
}

func (m *mp[T]) Delete(id string) {
	m.l.Lock()
	defer m.l.Unlock()
	delete(m.data, id)
}

func (m *mp[T]) Range(f func(k string, v T) bool) {
	m.l.Lock()
	d := m.data
	m.l.Unlock()

	for k, v := range d {
		if f(k, v) {
			continue
		}
		break
	}
}
