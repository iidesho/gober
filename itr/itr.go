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
	Iterator[V any]    iter.Seq[V]
	ErrIterator[V any] iter.Seq[okerr[V]]
	EnuIterator[V any] iter.Seq2[int, V]
)

type okerr[T any] struct {
	ok  T
	err error
}

func FromSeq1[V any](seq iter.Seq[V]) Iterator[V] {
	return Iterator[V](seq)
}

func NewIterator[V any](arr []V) Iterator[V] {
	return func(yield func(V) bool) {
		for _, v := range arr {
			if !yield(v) {
				break
			}
		}
	}
}

func NewMapKeysIterator[K comparable, V any](arr map[K]V) Iterator[K] {
	return func(yield func(K) bool) {
		for v := range arr {
			if !yield(v) {
				break
			}
		}
	}
}

func NewMapValuesIterator[K comparable, V any](arr map[K]V) Iterator[V] {
	return func(yield func(V) bool) {
		for _, v := range arr {
			if !yield(v) {
				break
			}
		}
	}
}

func NewGenerator[V any](g func() (V, bool)) Iterator[V] {
	return func(yield func(V) bool) {
		for {
			v, ok := g()
			if !ok {
				break
			}
			if !yield(v) {
				break
			}
		}
	}
}

func (seq Iterator[V]) Filter(keep func(s V) bool) Iterator[V] {
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

func (seq ErrIterator[V]) Filter(keep func(s V) bool) ErrIterator[V] {
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

func (seq ErrIterator[V]) FilterOk() Iterator[V] {
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

func (seq ErrIterator[V]) FilterErr() Iterator[error] {
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

func (seq Iterator[V]) Last() V {
	var v V
	for v = range seq {
	}
	return v
}

func (seq ErrIterator[V]) Last() V {
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

func (seq Iterator[V]) First() (v V) {
	for v := range seq {
		return v
	}
	return
}

func (seq ErrIterator[V]) First() (v V) {
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

func (seq Iterator[V]) Count() int {
	c := 0
	for range seq {
		c++
	}
	return c
}

func (seq ErrIterator[V]) Count() int {
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

func (seq Iterator[V]) Collect() []V {
	s := []V{}
	for v := range seq {
		s = append(s, v)
	}
	return s
}

func (seq ErrIterator[V]) CollectOk(log sbragi.ErrorLogger, lvl slog.Level) []V {
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

func (seq ErrIterator[V]) CollectErr() []error {
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

func (seq Iterator[V]) CollectMap(key func(v V) string) map[string]V {
	s := map[string]V{}
	for v := range seq {
		s[key(v)] = v
	}
	return s
}

func (seq Iterator[V]) Contains(eq func(v V) bool) bool {
	contains := false
	for v := range seq {
		if eq(v) {
			contains = true
			break
		}
	}
	return contains
}

func (seq EnuIterator[V]) Contains(eq func(v V) bool) int {
	for i, v := range seq {
		if eq(v) {
			return i
		}
	}
	return -1
}

func (seq ErrIterator[V]) Contains(eq func(v V) bool) bool {
	return seq.FilterOk().Contains(eq)
}

func (seq Iterator[V]) Unique() Iterator[V] {
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

func (seq Iterator[V]) Deduplicate(eq func(v1, v2 V) bool) Iterator[V] {
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

func (seq ErrIterator[V]) Deduplicate(eq func(v1, v2 V) bool) ErrIterator[V] {
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

func (seq Iterator[V]) EQ(seq2 Iterator[V], eq func(v1, v2 V) bool) bool {
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

func (seq Iterator[V]) Sort(swap func(v1, v2 V) bool) Iterator[V] {
	// Sholid keep the errors and not create a new iterator
	s := seq.Collect()
	sort.SliceStable(s, func(i, j int) bool {
		return swap(s[i], s[j])
	})
	return NewIterator(s)
}

func (seq Iterator[V]) Transform(trans func(s V) (V, error)) ErrIterator[V] {
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

func (seq ErrIterator[V]) Transform(trans func(s V) (V, error)) ErrIterator[V] {
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

func (seq Iterator[V]) TransformToInt(trans func(s V) (int, error)) ErrIterator[int] {
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

func (seq ErrIterator[V]) TransformToInt(trans func(s V) (int, error)) ErrIterator[int] {
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

func (seq Iterator[V]) TransformToString(trans func(s V) (string, error)) ErrIterator[string] {
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

func (seq ErrIterator[V]) TransformToString(trans func(s V) (string, error)) ErrIterator[string] {
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

func (seq Iterator[V]) Enumerate() EnuIterator[V] {
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

func (seq ErrIterator[V]) EnumerateOk() EnuIterator[V] {
	return seq.FilterOk().Enumerate()
}

func (seq ErrIterator[V]) EnumerateErr() EnuIterator[error] {
	return seq.FilterErr().Enumerate()
}
