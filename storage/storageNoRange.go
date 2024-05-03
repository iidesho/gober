//go:build !goexperiment.rangefunc

package storage

type Storage[T any] interface {
	Set(k string, v T, opts ...OptFunc) error
	Get(k string) (v T, err error)
	Delete(k string, opts ...OptFunc) error
}
