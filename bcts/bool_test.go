package bcts_test

import (
	"bytes"
	"testing"

	"github.com/iidesho/gober/bcts"
)

func BenchmarkBool(b *testing.B) {
	bw1, bw2, bw3, bw4, bw5, bw6, bw7, bw8 := bcts.ExtractBoolsFromUint8(255)
	var br1, br2, br3, br4, br5, br6, br7, br8 bool
	b.ResetTimer()
	for range b.N {
		v := bcts.BoolsToUint8(bw1, bw2, bw3, bw4, bw5, bw6, bw7, bw8)
		//br1, br2, br3, br4, br5, br6, br7, br8 = bcts.ExtractBoolsFromUint8(v)
		bcts.Uint8ToBools(v, &br1, &br2, &br3, &br4, &br5, &br6, &br7, &br8)
		if br1 != bw1 &&
			br2 != bw2 &&
			br3 != bw3 &&
			br4 != bw4 &&
			br5 != bw5 &&
			br6 != bw6 &&
			br7 != bw7 &&
			br8 != bw8 {
			b.Fatal("not equal read and write")
		}
	}
}

func BenchmarkBoolLoop(b *testing.B) {
	b.ResetTimer()
	var v uint8
	var b1, b2, b3, b4, b5, b6, b7, b8 bool
	for range b.N {
		bcts.Uint8ToBools(v, &b1, &b2, &b3, &b4, &b5, &b6, &b7, &b8)
		v2 := bcts.BoolsToUint8(b1, b2, b3, b4, b5, b6, b7, b8)
		//br1, br2, br3, br4, br5, br6, br7, br8 = bcts.ExtractBoolsFromUint8(v)
		if v != v2 {
			b.Fatal("not equal read and write")
		}
		v++
	}
}

func BenchmarkBoolFull(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 1))
	var err error
	bw1, bw2, bw3, bw4, bw5, bw6, bw7, bw8 := bcts.ExtractBoolsFromUint8(128)
	var br1, br2, br3, br4, br5, br6, br7, br8 bool
	for range b.N {
		err = bcts.WriteBools(buf, bw1, bw2, bw3, bw4, bw5, bw6, bw7, bw8)
		if err != nil {
			b.Fatal(err)
		}
		err = bcts.ReadBools(buf, &br1, &br2, &br3, &br4, &br5, &br6, &br7, &br8)
		if err != nil {
			b.Fatal(err)
		}
		if br1 != bw1 &&
			br2 != bw2 &&
			br3 != bw3 &&
			br4 != bw4 &&
			br5 != bw5 &&
			br6 != bw6 &&
			br7 != bw7 &&
			br8 != bw8 {
			b.Fatal("not equal read and write")
		}
		buf.Reset()
	}
}
