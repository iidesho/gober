package tasks

import "sync"

type mp[T any] struct {
	l    sync.Mutex
	data map[string]T
}

type Map[T any] interface {
	Store(id string, data T)
	Load(id string) (data T, ok bool)
	Delete(id string)
	Range(f func(k, v any) bool)
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

func (m *mp[T]) Range(f func(k, v any) bool) {
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
