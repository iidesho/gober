package bcts

import (
	"bufio"
	"bytes"
	"io"
)

func Write(w Writer) ([]byte, error) {
	dByte := bytes.NewBuffer([]byte{})
	dBw := bufio.NewWriter(dByte)
	err := w.WriteBytes(dBw)
	if err != nil {
		return nil, err
	}
	err = dBw.Flush()
	if err != nil {
		return nil, err
	}
	out := dByte.Bytes()
	return out, nil
}

func ReadAnyReader[BT any, T Reader[BT]](r io.Reader, v *T) error {
	*v = new(BT)
	return (*v).ReadBytes(r)
}

func ReadReader[BT any, T Reader[BT]](r io.Reader) (BT, error) {
	bv := new(BT)
	v := T(bv)
	err := v.ReadBytes(r)
	return *v, err
}

func Read[BT any, T Reader[BT]](data []byte) (T, error) {
	dByte := bytes.NewReader(data)
	v := T(new(BT))
	err := v.ReadBytes(dByte)
	return v, err
}
