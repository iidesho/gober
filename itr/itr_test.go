package itr_test

import (
	"fmt"
	"testing"

	"github.com/iidesho/gober/itr"
)

func TestGeneratorEnumerator(t *testing.T) {
	g := -1
	itr := itr.NewGenerator(func() string {
		g++
		return fmt.Sprint("test", g)
	}).EnumerateOk()
	var i, i2 int
	var v string
	for i, v = range itr {
		fmt.Println(1, i, v)
		if i != i2 {
			t.Fatal("i != i2", i, i2)
		}
		if i >= 10 {
			break
		}
		i2++
	}
	if i != 10 {
		t.Fatal("1 i, v", i, v)
	}
	for i, v = range itr {
		fmt.Println(2, i, v)
		if i >= 10 {
			break
		}
	}
	if i != 11 {
		t.Fatal("2 i, v", i, v)
	}
	itr(func(i int, v string) bool {
		fmt.Println(3, i, v)
		if i != 12 {
			t.Fatal("3 i, v", i, v)
		}
		return false
	})
	itr(func(i int, v string) bool {
		fmt.Println(4, i, v)
		if i >= 20 {
			t.Fatal("4 i, v", i, v)
		}
		return i < 19
	})
	itr(func(i int, v string) bool {
		fmt.Println(5, i, v)
		if i != 20 {
			t.Fatal("5 i, v", i, v)
		}
		return false
	})
}
