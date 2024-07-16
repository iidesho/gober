package bcts

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"time"
)

func WriteInt64[T ~int64](w *bufio.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteInt32[T ~int32](w *bufio.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteInt16[T ~int16](w *bufio.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteInt8[T ~int8](w *bufio.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteUInt64[T ~uint64](w *bufio.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteUInt32[T ~uint32](w *bufio.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteUInt16[T ~uint16](w *bufio.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteUInt8[T ~uint8](w *bufio.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

const maxUint8 = ^uint8(0)
const maxUint16 = ^uint16(0)
const maxUint32 = ^uint32(0)
const maxUint64 = ^uint64(0)

func WriteTinyString[T ~string](w *bufio.Writer, s T) error {
	l := len(s)
	if l > int(maxUint8) {
		return fmt.Errorf("string is longer than max length of a tiny string")
	}
	err := WriteUInt8(w, uint8(l))
	if err != nil {
		return err
	}
	if l == 0 {
		return nil
	}
	_, err = w.WriteString(string(s))
	return err
}

func WriteSmallString[T ~string](w *bufio.Writer, s T) error {
	l := len(s)
	if l > int(maxUint16) {
		return fmt.Errorf("string is longer than max length of a small string")
	}
	err := WriteUInt16(w, uint16(l))
	if err != nil {
		return err
	}
	if l == 0 {
		return nil
	}
	_, err = w.WriteString(string(s))
	return err
}

func WriteString[T ~string](w *bufio.Writer, s T) error {
	l := len(s)
	if l > int(maxUint32) {
		return fmt.Errorf("string is longer than max length of a long string")
	}
	err := WriteUInt32(w, uint32(l))
	if err != nil {
		return err
	}
	if l == 0 {
		return nil
	}
	_, err = w.WriteString(string(s))
	return err
}

func WriteTinyBytes(w *bufio.Writer, b []byte) error {
	l := len(b)
	if l > int(maxUint8) {
		return fmt.Errorf("byte slice is longer than max length of a tiny byte slice")
	}
	err := WriteUInt8(w, uint8(l))
	if err != nil {
		return err
	}
	if l == 0 {
		return nil
	}
	_, err = w.Write(b)
	return err
}

func WriteSmallBytes(w *bufio.Writer, b []byte) error {
	l := len(b)
	if l > int(maxUint16) {
		return fmt.Errorf("byte slice is longer than max length of a small byte slice")
	}
	err := WriteUInt16(w, uint16(l))
	if err != nil {
		return err
	}
	if l == 0 {
		return nil
	}
	_, err = w.Write(b)
	return err
}

func WriteBytes(w *bufio.Writer, b []byte) error {
	l := uint32(len(b))
	err := WriteUInt32(w, l)
	if err != nil {
		return err
	}
	if l == 0 {
		return nil
	}
	_, err = w.Write(b)
	return err
}

func WriteStaticBytes(w *bufio.Writer, b []byte) error {
	_, err := w.Write(b)
	return err
}

func WriteTime(w *bufio.Writer, t time.Time) error {
	return WriteInt64(w, t.UTC().UnixNano())
}

func WriteMap[K ComparableWriter, T Writer](w *bufio.Writer, m map[K]T) error {
	err := WriteInt32(w, int32(len(m)))
	if err != nil {
		return err
	}
	for k, v := range m {
		err = k.WriteBytes(w)
		if err != nil {
			return err
		}
		err = v.WriteBytes(w)
		if err != nil {
			return err
		}
	}
	return nil
}
