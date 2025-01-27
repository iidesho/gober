package itr

import (
	"iter"
	"log/slog"
	"reflect"
	"sort"

	"github.com/iidesho/bragi/sbragi"
)

var log = sbragi.WithLocalScope(sbragi.LevelError)

type (
	iterator[V any]    iter.Seq[V]
	errIterator[V any] iter.Seq[okerr[V]]
	enuIterator[V any] iter.Seq2[int, V]
)

type okerr[T any] struct {
	ok  T
	err error
}

func FromSeq1[V any](seq iter.Seq[V]) iterator[V] {
	return iterator[V](seq)
}

func NewIterator[V any](arr []V) iterator[V] {
	return func(yield func(V) bool) {
		for _, v := range arr {
			if !yield(v) {
				break
			}
		}
	}
}

func NewMapKeysIterator[K comparable, V any](arr map[K]V) iterator[K] {
	return func(yield func(K) bool) {
		for v := range arr {
			if !yield(v) {
				break
			}
		}
	}
}

func NewMapValuesIterator[K comparable, V any](arr map[K]V) iterator[V] {
	return func(yield func(V) bool) {
		for _, v := range arr {
			if !yield(v) {
				break
			}
		}
	}
}

func NewGenerator[V any](g func() V) iterator[V] {
	return func(yield func(V) bool) {
		for yield(g()) {
		}
	}
}

func (seq iterator[V]) Filter(keep func(s V) bool) iterator[V] {
	return func(yield func(V) bool) {
		for v := range seq {
			if !keep(v) {
				continue
			}

			if !yield(v) {
				break
			}
		}
	}
}

func (seq errIterator[V]) Filter(keep func(s V) bool) errIterator[V] {
	return func(yield func(okerr[V]) bool) {
		for v := range seq {
			if v.err == nil {
				if !keep(v.ok) {
					continue
				}
			}

			if !yield(v) {
				break
			}
		}
	}
}

func (seq errIterator[V]) FilterOk() iterator[V] {
	return func(yield func(V) bool) {
		for v := range seq {
			if v.err != nil {
				continue
			}

			if !yield(v.ok) {
				break
			}
		}
	}
}

func (seq errIterator[V]) FilterErr() iterator[error] {
	return func(yield func(error) bool) {
		for v := range seq {
			if v.err == nil {
				continue
			}

			if !yield(v.err) {
				break
			}
		}
	}
}

func (seq iterator[V]) Last() V {
	var v V
	for v = range seq {
	}
	return v
}

func (seq errIterator[V]) Last() V {
	var v V
	for o := range seq {
		if o.err != nil {
			continue
		}
		v = o.ok
	}
	return v
}

/*
func (seq iterator[V]) Pop() (vo V) {
	seq(func(vi V) bool {
		vo = vi
		return false
	})
	return
}
*/

func (seq iterator[V]) First() (v V) {
	for v := range seq {
		return v
	}
	return
}

func (seq errIterator[V]) First() (v V) {
	for o := range seq {
		if o.err != nil {
			continue
		}
		return o.ok
	}
	/*
		var v V
		seq(func(v2 V) bool {
			v = v2
			return false
		})
		return v
	*/
	return // If no ok value exists we return Zero type
}

func (seq iterator[V]) Count() int {
	c := 0
	for range seq {
		c++
	}
	return c
}

func (seq errIterator[V]) Count() int {
	c := 0
	for range seq {
		c++
	}
	return c
}

/*
func (seq iterator[int]) Max() int {
	c := 0
	for v := range seq {
		if v > c {
			c = v
		}
	}
	return c
}
*/

func (seq iterator[V]) Collect() []V {
	s := []V{}
	for v := range seq {
		s = append(s, v)
	}
	return s
}

func (seq errIterator[V]) CollectOk(log sbragi.ErrorLogger, lvl slog.Level) []V {
	s := []V{}
	for o := range seq {
		if o.err != nil {
			log.WithoutEscalation().WithError(o.err).Level(lvl, "hidden iter error")
			continue
		}
		s = append(s, o.ok)
	}
	return s
}

func (seq errIterator[V]) CollectErr() []error {
	s := []error{}
	for o := range seq {
		if o.err == nil {
			continue
		}
		s = append(s, o.err)
	}
	return s
}

/*
todo
func (seq iterator[V]) CollectAll() []error {
	s := []error{}
	for o := range seq {
		if o.err == nil {
			continue
		}
		s = append(s, o.err)
	}
	return s
}
*/

func (seq iterator[V]) CollectMap(key func(v V) string) map[string]V {
	s := map[string]V{}
	for v := range seq {
		s[key(v)] = v
	}
	return s
}

func (seq iterator[V]) Contains(eq func(v V) bool) bool {
	contains := false
	for v := range seq {
		if eq(v) {
			contains = true
			break
		}
	}
	return contains
}

func (seq enuIterator[V]) Contains(eq func(v V) bool) int {
	for i, v := range seq {
		if eq(v) {
			return i
		}
	}
	return -1
}

func (seq errIterator[V]) Contains(eq func(v V) bool) bool {
	return seq.FilterOk().Contains(eq)
}

func (seq iterator[V]) Unique() iterator[V] {
	seen := []V{}
	return func(yield func(V) bool) {
		for v := range seq {
			if NewIterator(seen).Contains(func(v2 V) bool {
				return reflect.DeepEqual(v, v2)
			}) {
				continue
			}
			seen = append(seen, v)
			if !yield(v) {
				break
			}
		}
	}
}

func (seq iterator[V]) Deduplicate(eq func(v1, v2 V) bool) iterator[V] {
	seen := []V{}
	return func(yield func(V) bool) {
		for v := range seq {
			if NewIterator(seen).Contains(func(v2 V) bool {
				return eq(v, v2)
			}) {
				continue
			}
			seen = append(seen, v)
			if !yield(v) {
				break
			}
		}
	}
}

func (seq errIterator[V]) Deduplicate(eq func(v1, v2 V) bool) errIterator[V] {
	seen := []V{}
	return func(yield func(okerr[V]) bool) {
		for o := range seq {
			if o.err != nil {
				if !yield(o) {
					break
				}
				continue
			}
			if NewIterator(seen).Contains(func(v V) bool {
				return eq(o.ok, v)
			}) {
				continue
			}
			seen = append(seen, o.ok)
			if !yield(o) {
				break
			}
		}
	}
}

func (seq iterator[V]) EQ(seq2 iterator[V], eq func(v1, v2 V) bool) bool {
	c1 := seq.Collect()
	c2 := seq2.Collect()
	if len(c1) != len(c2) {
		return false
	}
	for i := range c1 {
		if !eq(c1[i], c2[i]) {
			return false
		}
	}
	return true
}

func (seq iterator[V]) Sort(swap func(v1, v2 V) bool) iterator[V] {
	// Sholid keep the errors and not create a new iterator
	s := seq.Collect()
	sort.SliceStable(s, func(i, j int) bool {
		return swap(s[i], s[j])
	})
	return NewIterator(s)
}

func (seq iterator[V]) Transform(trans func(s V) (V, error)) errIterator[V] {
	return func(yield func(okerr[V]) bool) {
		for v := range seq {

			var o okerr[V]
			o.ok, o.err = trans(v)

			if !yield(o) {
				break
			}
		}
	}
}

func (seq errIterator[V]) Transform(trans func(s V) (V, error)) errIterator[V] {
	return func(yield func(okerr[V]) bool) {
		for o := range seq {
			if o.err == nil {
				o.ok, o.err = trans(o.ok)
			}

			if !yield(o) {
				break
			}
		}
	}
}

func (seq iterator[V]) TransformToInt(trans func(s V) (int, error)) errIterator[int] {
	return func(yield func(okerr[int]) bool) {
		for v := range seq {
			var ou okerr[int]
			ou.ok, ou.err = trans(v)

			if !yield(ou) {
				break
			}
		}
	}
}

func (seq errIterator[V]) TransformToInt(trans func(s V) (int, error)) errIterator[int] {
	return func(yield func(okerr[int]) bool) {
		for oi := range seq {
			var ou okerr[int]
			if oi.err == nil {
				ou.ok, ou.err = trans(oi.ok)
			} else {
				ou.err = oi.err
			}

			if !yield(ou) {
				break
			}
		}
	}
}

func (seq iterator[V]) TransformToString(trans func(s V) (string, error)) errIterator[string] {
	return func(yield func(okerr[string]) bool) {
		for v := range seq {
			var ou okerr[string]
			ou.ok, ou.err = trans(v)

			if !yield(ou) {
				break
			}
		}
	}
}

func (seq errIterator[V]) TransformToString(trans func(s V) (string, error)) errIterator[string] {
	return func(yield func(okerr[string]) bool) {
		for oi := range seq {
			var ou okerr[string]
			if oi.err == nil {
				ou.ok, ou.err = trans(oi.ok)
			} else {
				ou.err = oi.err
			}

			if !yield(ou) {
				break
			}
		}
	}
}

func keys[T any](m map[string]T) []string {
	out := make([]string, len(m))
	i := 0
	for k := range m {
		out[i] = k
		i++
	}
	return out
}

func (seq iterator[V]) Enumerate() enuIterator[V] {
	i := -1
	return func(yield func(int, V) bool) {
		for v := range seq {
			i++
			if !yield(i, v) {
				break
			}
		}
	}
}

func (seq errIterator[V]) EnumerateOk() enuIterator[V] {
	return seq.FilterOk().Enumerate()
}

func (seq errIterator[V]) EnumerateErr() enuIterator[error] {
	return seq.FilterErr().Enumerate()
}
