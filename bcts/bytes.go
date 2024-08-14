package bcts

import (
	"io"
)

type (
	TinyBytes  []byte
	SmallBytes []byte
	Bytes      []byte
)

func (b TinyBytes) WriteBytes(w io.Writer) error {
	return WriteTinyBytes(w, b)
}

func (b *TinyBytes) ReadBytes(r io.Reader) error {
	return ReadTinyBytes(r, b)
}

func (b SmallBytes) WriteBytes(w io.Writer) error {
	return WriteSmallBytes(w, b)
}

func (b *SmallBytes) ReadBytes(r io.Reader) error {
	return ReadSmallBytes(r, b)
}

func (b Bytes) WriteBytes(w io.Writer) error {
	return WriteBytes(w, b)
}

func (b *Bytes) ReadBytes(r io.Reader) error {
	return ReadBytes(r, b)
}
