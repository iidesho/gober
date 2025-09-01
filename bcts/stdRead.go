package bcts

import (
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/gofrs/uuid"
)

// I hate that we do not have the ability to either write single bit or do better bitwize operations...
func ExtractBoolsFromUint8(u uint8) (b1, b2, b3, b4, b5, b6, b7, b8 bool) {
	b1 = u&128 > 0 // 1<<7
	b2 = u&64 > 0  // 1<<6
	b3 = u&32 > 0  // 1<<5
	b4 = u&16 > 0  // 1<<4
	b5 = u&8 > 0   // 1<<3
	b6 = u&4 > 0   // 1<<2
	b7 = u&2 > 0   // 1<<1
	b8 = u&1 > 0   // 1<<0
	return
}

func Uint8ToBools[T1 ~bool, T2 ~bool, T3 ~bool, T4 ~bool, T5 ~bool, T6 ~bool, T7 ~bool, T8 ~bool](
	u uint8,
	b1 *T1, b2 *T2, b3 *T3, b4 *T4, b5 *T5, b6 *T6, b7 *T7, b8 *T8,
) {
	if b1 != nil {
		*b1 = u&128 > 0 // 1<<7
	}
	if b2 != nil {
		*b2 = u&64 > 0 // 1<<6
	}
	if b3 != nil {
		*b3 = u&32 > 0 // 1<<5
	}
	if b4 != nil {
		*b4 = u&16 > 0 // 1<<4
	}
	if b5 != nil {
		*b5 = u&8 > 0 // 1<<3
	}
	if b6 != nil {
		*b6 = u&4 > 0 // 1<<2
	}
	if b7 != nil {
		*b7 = u&2 > 0 // 1<<1
	}
	if b8 != nil {
		*b8 = u&1 > 0 // 1<<0
	}
}

func ReadBools(r io.Reader, b1, b2, b3, b4, b5, b6, b7, b8 *bool) error {
	var u uint8
	err := ReadUInt8(r, &u)
	if err != nil {
		return err
	}
	//*b1, *b2, *b3, *b4, *b5, *b6, *b7, *b8 = ExtractBoolsFromUint8(v)
	// Inlining as pointers can be nil
	Uint8ToBools(u, b1, b2, b3, b4, b5, b6, b7, b8)
	return nil
}

func ReadInt64[T ~int64](r io.Reader, i *T) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func ReadInt32[T ~int32](r io.Reader, i *T) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func ReadInt16[T ~int16](r io.Reader, i *T) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func ReadInt8[T ~int8](r io.Reader, i *T) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func ReadUInt64[T ~uint64](r io.Reader, i *T) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func ReadUInt32[T ~uint32](r io.Reader, i *T) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func ReadUInt16[T ~uint16](r io.Reader, i *T) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func ReadUInt8[T ~uint8](r io.Reader, i *T) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func ReadTinyString[T ~string](r io.Reader, s *T) error {
	var l uint8
	err := ReadUInt8(r, &l)
	if err != nil {
		return err
	}
	buf := make([]byte, l)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return err
	}
	*s = T(buf)
	return nil
}

func ReadSmallString[T ~string](r io.Reader, s *T) error {
	var l uint16
	err := ReadUInt16(r, &l)
	if err != nil {
		return err
	}
	buf := make([]byte, l)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return err
	}
	*s = T(buf)
	return nil
}

func ReadString[T ~string](r io.Reader, s *T) error {
	var l uint32
	err := ReadUInt32(r, &l)
	if err != nil {
		return err
	}
	buf := make([]byte, l)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return err
	}
	*s = T(buf)
	return nil
}

func ReadTinyBytes[T ~[]byte](r io.Reader, b *T) error {
	var l uint8
	err := ReadUInt8(r, &l)
	if err != nil {
		return err
	}
	*b = make([]byte, l)
	_, err = io.ReadFull(r, *b)
	return err
}

func ReadSmallBytes[T ~[]byte](r io.Reader, b *T) error {
	var l uint16
	err := ReadUInt16(r, &l)
	if err != nil {
		return err
	}
	*b = make([]byte, l)
	_, err = io.ReadFull(r, *b)
	return err
}

func ReadBytes[T ~[]byte](r io.Reader, b *T) error {
	var l uint32
	err := ReadUInt32(r, &l)
	if err != nil {
		return err
	}
	*b = make([]byte, l)
	_, err = io.ReadFull(r, *b)
	return err
}

func ReadStaticBytes[T ~[]byte](r io.Reader, b T) error {
	_, err := io.ReadFull(r, b)
	return err
}

func ReadUUID(r io.Reader, b *uuid.UUID) error {
	_, err := io.ReadFull(r, b[:])
	return err
}

func ReadTime(r io.Reader, t *time.Time) error {
	var ns int64
	err := ReadInt64(r, &ns)
	if err != nil {
		return err
	}
	*t = time.Unix(0, ns)
	return nil
}

func ReadMap[KT comparable, K ComparableReader[KT], VT any, V Reader[VT]](
	r io.Reader,
	mp *map[KT]VT,
) error {
	*mp = map[KT]VT{}
	var l int32
	err := ReadInt32(r, &l)
	if err != nil {
		return err
	}
	for range l {
		k := new(KT)
		v := new(VT)
		err = K(k).ReadBytes(r)
		if err != nil {
			return err
		}
		err = V(v).ReadBytes(r)
		if err != nil {
			return err
		}
		(*mp)[*k] = *v
	}
	return nil
}

func ReadAny(r io.Reader, a any) error {
	switch a := a.(type) {
	// case *uint:
	// return ReadUInt32(r, a)
	case *uint8:
		return ReadUInt8(r, a)
	case *uint16:
		return ReadUInt16(r, a)
	case *uint32:
		return ReadUInt32(r, a)
	case *uint64:
		return ReadUInt64(r, a)
	// case *int:
	// return ReadInt32(r, int32(a))
	case *int8:
		return ReadInt8(r, a)
	case *int16:
		return ReadInt16(r, a)
	case *int32:
		return ReadInt32(r, a)
	case *int64:
		return ReadInt64(r, a)
	case *string:
		return ReadTinyString(r, a)
	case uuid.UUID:
		return ReadStaticBytes(r, a[:])
	case *time.Time:
		return ReadTime(r, a)
	default:
		return errors.New("unsuported any type")
	}
}

func ReadMapAny[K comparable, V any](
	r io.Reader,
	mp *map[K]V,
) error {
	*mp = map[K]V{}
	var l int32
	err := ReadInt32(r, &l)
	if err != nil {
		return err
	}
	for range l {
		k := new(K)
		v := new(V)
		err = ReadAny(r, k)
		if err != nil {
			return err
		}
		err = ReadAny(r, v)
		if err != nil {
			return err
		}
		(*mp)[*k] = *v
	}
	return nil
}

func ReadMapAnyFunc[K comparable, V any](
	r io.Reader,
	mp *map[K]V,
	keyReader func(r io.Reader, v *K) error,
	valReader func(r io.Reader, v *V) error,
) error {
	*mp = map[K]V{}
	var l int32
	err := ReadInt32(r, &l)
	if err != nil {
		return err
	}
	for range l {
		k := new(K)
		v := new(V)
		err = keyReader(r, k)
		if err != nil {
			return err
		}
		err = valReader(r, v)
		if err != nil {
			return err
		}
		(*mp)[*k] = *v
	}
	return nil
}

func ReadSlice[TV any, T Reader[TV]](r io.Reader, s *[]TV) error {
	var l int32
	err := ReadInt32(r, &l)
	if err != nil {
		return err
	}
	*s = make([]TV, l)
	for i := range l {
		v := new(TV)
		err = T(v).ReadBytes(r)
		if err != nil {
			return err
		}
		(*s)[i] = *v
	}
	return nil
}

func ReadSliceAny[TV any](r io.Reader, s *[]TV, t func(r io.Reader, v *TV) error) error {
	var l int32
	err := ReadInt32(r, &l)
	if err != nil {
		return err
	}
	*s = make([]TV, l)
	for i := range l {
		v := new(TV)
		err := t(r, v) // T(v).ReadBytes(r)
		if err != nil {
			return err
		}
		(*s)[i] = *v
	}
	return nil
}

func ReadError(r io.Reader, err *error) error {
	var errS string
	iErr := ReadSmallString(r, &errS)
	if err != nil {
		return iErr
	}
	if len(errS) == 0 {
		return nil
	}
	*err = errors.New(errS)
	return nil
}
