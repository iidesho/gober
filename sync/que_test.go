package sync

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/iidesho/gober/bcts"
)

func TestQue(t *testing.T) {
	q := NewQue[bcts.TinyString]()
	itm := bcts.TinyString("test_item")
	if v, ok := q.Peek(); ok {
		t.Fatal("peek returned ok when que is empty", "val", v)
	}
	q.Push(&itm)
	v, ok := q.Peek()
	if !ok {
		t.Fatal("peek returned not ok when que has itm")
	}
	if *v != itm {
		t.Fatal("que did not return expected item", "expected", itm, "got", v)
	}

	buf := bytes.NewBuffer([]byte{})
	w := bufio.NewWriter(buf)
	err := q.WriteBytes(w)
	if err != nil {
		t.Fatal(err)
	}
	err = w.Flush()
	if err != nil {
		t.Fatal(err)
	}
	q, err = QueFromReader[bcts.TinyString](buf)
	if err != nil {
		t.Fatal(err)
	}

	v, ok = q.Pop()
	if !ok {
		t.Fatal("pop returned not ok when que has itm")
	}
	if *v != itm {
		t.Fatal("que did not return expected item", "expected", itm, "got", v)
	}
	if v, ok := q.Peek(); ok {
		t.Fatal("peek returned ok when que is empty", "val", v)
	}
	select {
	case <-q.HasData():
		t.Fatal("has data signaled when que is empty")
	default:
	}
	q.Push(&itm)
	select {
	case <-q.HasData():
	default:
		t.Fatal("has data did not signal when que had data")
	}
	q.Delete(func(v *bcts.TinyString) bool { return *v == itm })
	if v, ok := q.Peek(); ok {
		t.Fatal("peek returned ok when que is empty", "val", v)
	}
	select {
	case <-q.HasData():
		t.Fatal("has data signaled when que is empty")
	default:
	}
}
