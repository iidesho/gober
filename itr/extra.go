package itr

func Max(seq iterator[int]) int {
	c := 0
	for v := range seq {
		if v > c {
			c = v
		}
	}
	return c
}

func Min(seq iterator[int]) int {
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

func Sum(seq iterator[int]) int {
	c := 0
	for v := range seq {
		c = c + v
	}
	return c
}
