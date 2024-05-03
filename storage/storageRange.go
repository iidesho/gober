//go:build goexperiment.rangefunc

package storage

import (
	"iter"

	"github.com/iidesho/bragi/sbragi"
	"github.com/nutsdb/nutsdb"
)

type Storage[T any] interface {
	Set(k string, v T, opts ...OptFunc) error
	Get(k string) (v T, err error)
	Delete(k string, opts ...OptFunc) error
	Range() iter.Seq2[string, T]
}

func (s storage[T]) Range() iter.Seq2[string, T] {
	var keys [][]byte
	var values [][]byte
	err := s.db.View(func(tx *nutsdb.Tx) error {
		var err error
		keys, values, err = tx.GetAll("bucket?")
		return err
	})
	sbragi.WithError(err).Error("getting values for range")
	return func(yield func(string, T) bool) {
		var v T
		for i := range keys {
			err = json.Unmarshal(values[i], &v)
			if sbragi.WithError(err).Error("unmarshaling value", "key", string(keys[i]), "raw_value", string(values[i])) {
				continue
			}
			if !yield(string(keys[i]), v) {
				return
			}
		}
	}
}
