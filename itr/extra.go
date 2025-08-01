package itr

import "iter"

func Max(seq Iterator[int]) int {
	c := 0
	for v := range seq {
		if v > c {
			c = v
		}
	}
	return c
}

func Min(seq Iterator[int]) int {
	c := 0
	for i, v := range seq.Enumerate() {
		if i == 0 {
			c = v
			continue
		}
		if v < c {
			c = v
		}
	}
	return c
}

func Sum(seq Iterator[int]) int {
	c := 0
	for v := range seq {
		c = c + v
	}
	return c
}

func Transform[INN, OUT any](seq Iterator[INN], trans func(INN) (OUT, error)) ErrIterator[OUT] {
	return func(yield func(okerr[OUT]) bool) {
		next, stop := iter.Pull(iter.Seq[INN](seq))
		defer stop()
		for {
			v, ok := next()
			if !ok {
				break
			}
			out, err := trans(v)
			if !yield(okerr[OUT]{
				ok:  out,
				err: err,
			}) {
				break
			}
		}
	}
}

func Append[V any](seq1, seq2 Iterator[V]) Iterator[V] {
	return func(yield func(V) bool) {
		for v := range seq1 {
			if !yield(v) {
				return
			}
		}
		for v := range seq2 {
			if !yield(v) {
				return
			}
		}
	}
}
