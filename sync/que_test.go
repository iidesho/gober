package sync

import "testing"

func TestQue(t *testing.T) {
	q := NewQue[string]()
	itm := "test_item"
	if v, ok := q.Peek(); ok {
		t.Fatal("peek returned ok when que is empty", "val", v)
	}
	q.Push(itm)
	v, ok := q.Peek()
	if !ok {
		t.Fatal("peek returned not ok when que has itm")
	}
	if v != itm {
		t.Fatal("que did not return expected item", "expected", itm, "got", v)
	}
	v, ok = q.Pop()
	if !ok {
		t.Fatal("pop returned not ok when que has itm")
	}
	if v != itm {
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
	q.Push(itm)
	select {
	case <-q.HasData():
	default:
		t.Fatal("has data did not signal when que had data")
	}
	q.Delete(func(v string) bool { return v == itm })
	if v, ok := q.Peek(); ok {
		t.Fatal("peek returned ok when que is empty", "val", v)
	}
	select {
	case <-q.HasData():
		t.Fatal("has data signaled when que is empty")
	default:
	}

}
