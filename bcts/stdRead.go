package bcts

import (
	"encoding/binary"
	"io"
	"time"
)

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

func ReadSlice[TV any, T Reader[TV]](r io.Reader, s *[]T) error {
	var l int32
	err := ReadInt32(r, &l)
	if err != nil {
		return err
	}
	*s = make([]T, l)
	for i := range l {
		v := new(TV)
		err = T(v).ReadBytes(r)
		if err != nil {
			return err
		}
		(*s)[i] = v
	}
	return nil
}
