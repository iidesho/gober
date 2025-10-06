package bcts

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/gofrs/uuid"
	contextkeys "github.com/iidesho/gober/contextKeys"
)

// I hate that we do not have the ability to either write single bit or do better bitwize operations...
func BoolsToUint8(b1, b2, b3, b4, b5, b6, b7, b8 bool) (u uint8) {
	if b1 {
		u = 1
	}
	u = u << 1
	if b2 {
		u = u | 1
	}
	u = u << 1
	if b3 {
		u = u | 1
	}
	u = u << 1
	if b4 {
		u = u | 1
	}
	u = u << 1
	if b5 {
		u = u | 1
	}
	u = u << 1
	if b6 {
		u = u | 1
	}
	u = u << 1
	if b7 {
		u = u | 1
	}
	u = u << 1
	if b8 {
		return u | 1
	}
	return
}

func WriteBools(w io.Writer, b1, b2, b3, b4, b5, b6, b7, b8 bool) error {
	return WriteUInt8(w, BoolsToUint8(b1, b2, b3, b4, b5, b6, b7, b8))
}

func WriteInt64[T ~int64](w io.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteInt32[T ~int32](w io.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteInt16[T ~int16](w io.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteInt8[T ~int8](w io.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteUInt64[T ~uint64](w io.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteUInt32[T ~uint32](w io.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteUInt16[T ~uint16](w io.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func WriteUInt8[T ~uint8](w io.Writer, i T) error {
	return binary.Write(w, binary.LittleEndian, i)
}

const (
	maxUint8  = ^uint8(0)
	maxUint16 = ^uint16(0)
	maxUint32 = ^uint32(0)
	maxUint64 = ^uint64(0)
)

func WriteTinyString[T ~string](w io.Writer, s T) error {
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
	return writeAll(w, []byte(s))
	// _, err = w.Write([]byte(s))
	// return err
}

func WriteSmallString[T ~string](w io.Writer, s T) error {
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
	return writeAll(w, []byte(s))
	// _, err = w.Write([]byte(s))
	// return err
}

func WriteString[T ~string](w io.Writer, s T) error {
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
	return writeAll(w, []byte(s))
	// _, err = w.Write([]byte(s))
	// return err
}

func WriteTinyBytes(w io.Writer, b []byte) error {
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
	return writeAll(w, b)
	// _, err = w.Write(b)
	// return err
}

func WriteSmallBytes(w io.Writer, b []byte) error {
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
	return writeAll(w, b)
	// _, err = w.Write(b)
	// return err
}

func WriteBytes(w io.Writer, b []byte) error {
	l := uint32(len(b))
	err := WriteUInt32(w, l)
	if err != nil {
		return err
	}
	if l == 0 {
		return nil
	}
	return writeAll(w, b)
	// _, err = w.Write(b)
	// return err
}

func WriteStaticBytes(w io.Writer, b []byte) error {
	return writeAll(w, b)
	// _, err := w.Write(b)
	// return err
}

func WriteUUID(w io.Writer, b uuid.UUID) error {
	return writeAll(w, b[:])
	// _, err := w.Write(b[:])
	// return err
}

func WriteTime(w io.Writer, t time.Time) error {
	return WriteInt64(w, t.UTC().UnixNano())
}

func WriteMap[K ComparableWriter, T Writer](w io.Writer, m map[K]T) error {
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

func WriteAny(w io.Writer, a any) error {
	switch a := a.(type) {
	case uint:
		err := WriteUInt16(w, typeUint)
		if err != nil {
			return err
		}
		return WriteUInt32(w, uint32(a))
	case uint8:
		err := WriteUInt16(w, typeUint8)
		if err != nil {
			return err
		}
		return WriteUInt8(w, a)
	case uint16:
		err := WriteUInt16(w, typeUint16)
		if err != nil {
			return err
		}
		return WriteUInt16(w, a)
	case uint32:
		err := WriteUInt16(w, typeUint32)
		if err != nil {
			return err
		}
		return WriteUInt32(w, a)
	case uint64:
		err := WriteUInt16(w, typeUint64)
		if err != nil {
			return err
		}
		return WriteUInt64(w, a)
	case int:
		err := WriteUInt16(w, typeInt)
		if err != nil {
			return err
		}
		return WriteInt32(w, int32(a))
	case int8:
		err := WriteUInt16(w, typeInt8)
		if err != nil {
			return err
		}
		return WriteInt8(w, a)
	case int16:
		err := WriteUInt16(w, typeInt16)
		if err != nil {
			return err
		}
		return WriteInt16(w, a)
	case int32:
		err := WriteUInt16(w, typeInt32)
		if err != nil {
			return err
		}
		return WriteInt32(w, a)
	case int64:
		err := WriteUInt16(w, typeInt64)
		if err != nil {
			return err
		}
		return WriteInt64(w, a)
	case string:
		err := WriteUInt16(w, typeString)
		if err != nil {
			return err
		}
		return WriteSmallString(w, a)
	case uuid.UUID:
		err := WriteUInt16(w, typeUUID)
		if err != nil {
			return err
		}
		return WriteStaticBytes(w, a[:])
	case time.Time:
		err := WriteUInt16(w, typeTime)
		if err != nil {
			return err
		}
		return WriteTime(w, a)
	// case fmt.Stringer:
	case contextkeys.ContextKey:
		err := WriteUInt16(w, typeContextKey)
		if err != nil {
			return err
		}
		return WriteSmallString(w, a)
	default:
		return fmt.Errorf("unsuported any type, %T", a)
	}
}

func WriteMapAny[K comparable, T any](
	w io.Writer,
	m map[K]T,
) error {
	err := WriteInt32(w, int32(len(m)))
	if err != nil {
		return err
	}
	for k, v := range m {
		err = WriteAny(w, k)
		if err != nil {
			return err
		}
		err = WriteAny(w, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteMapAnyFunc[K comparable, T any](
	w io.Writer,
	m map[K]T,
	writeKey func(w io.Writer, v K) error,
	writeVal func(w io.Writer, v T) error,
) error {
	err := WriteInt32(w, int32(len(m)))
	if err != nil {
		return err
	}
	for k, v := range m {
		err = writeKey(w, k)
		if err != nil {
			return err
		}
		err = writeVal(w, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteSlice[T Writer, TI ~[]T](w io.Writer, s TI) error {
	if len(s) > int(maxUint32) {
		return fmt.Errorf("slice length is too long")
	}
	err := WriteUInt32(w, uint32(len(s)))
	if err != nil {
		return err
	}
	for _, v := range s {
		err = v.WriteBytes(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteSliceAny[T any, TI ~[]T](w io.Writer, s TI, t func(w io.Writer, s T) error) error {
	if len(s) > int(maxUint32) {
		return fmt.Errorf("slice length is too long")
	}
	err := WriteUInt32(w, uint32(len(s)))
	if err != nil {
		return err
	}
	for _, v := range s {
		err = t(w, v) // v.WriteBytes(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteError(w io.Writer, err error) error {
	if err == nil {
		return WriteUInt16(w, uint16(0))
	}
	err = WriteSmallString(w, err.Error())
	if err != nil {
		return err
	}
	return nil
}

func writeAll(w io.Writer, b []byte) error {
	written := 0
	for written < len(b) {
		n, err := w.Write(b[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}
