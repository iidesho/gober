package sync

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/iidesho/gober/bcts"
)

var smap *Map[bcts.TinyString, *bcts.TinyString]

func TestNew(t *testing.T) {
	smap = NewMap[bcts.TinyString, *bcts.TinyString]()
	/*
		if smap == nil {
			t.Error("unable to create map")
			return
		}
	*/
}

var baseString = bcts.TinyString("19")

func TestSyncMap_Set(t *testing.T) {
	smap.Set("key", &baseString)
}

func TestSyncMap_Get(t *testing.T) {
	i, ok := smap.Get("key")
	if !ok {
		t.Error("get not okay")
		return
	}
	if *i != baseString {
		t.Error("data not equal, expected 19 got ", i)
		return
	}
}

func TestSyncMap_WriteReadBytes(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	w := bufio.NewWriter(buf)
	err := smap.WriteBytes(w)
	if err != nil {
		t.Fatal(err)
	}
	w.Flush()
	if err != nil {
		t.Fatal(err)
	}
	smap, err = MapFromReader[bcts.TinyString](buf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSyncMap_GetMap(t *testing.T) {
	m := smap.GetMap()
	for k, v := range m {
		if k != "key" {
			t.Error("key if not key, ", k)
			return
		}
		if *v != baseString {
			t.Error("data not equal, expected 19 got ", v)
		}
	}
}

var bsmap = NewMap[bcts.TinyString]()

func BenchmarkMultiReadWrite(b *testing.B) {
	go func() {
		v, _ := bsmap.Get("k")
		var i int
		if v != nil {
			i, _ = strconv.Atoi(string(*v))
		}
		nv := bcts.TinyString(fmt.Sprintf("%d", i+1))
		bsmap.Set("k", &nv)
	}()
	var wg sync.WaitGroup
	wg.Add(b.N)
	for i := 0; i < b.N; i++ {
		go func(i int) {
			defer wg.Done()
			nv := bcts.TinyString(strconv.Itoa(i))
			m := bsmap.GetMap()
			for k, v := range m {
				bsmap.Set(bcts.TinyString(fmt.Sprintf("%v-%v", k, v)), &nv)
			}
		}(i)
	}
	wg.Wait()
	m := bsmap.GetMap()
	for k, v := range m {
		fmt.Println(k, v)
	}
}
