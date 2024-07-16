package bcts

import (
	"bufio"
	"io"
)

type TinyBytes []byte
type SmallBytes []byte
type Bytes []byte

func (b TinyBytes) WriteBytes(w *bufio.Writer) error {
	return WriteTinyBytes(w, b)
}

func (b *TinyBytes) ReadBytes(r io.Reader) error {
	return ReadTinyBytes(r, b)
}

func (b SmallBytes) WriteBytes(w *bufio.Writer) error {
	return WriteSmallBytes(w, b)
}

func (b *SmallBytes) ReadBytes(r io.Reader) error {
	return ReadSmallBytes(r, b)
}

func (b Bytes) WriteBytes(w *bufio.Writer) error {
	return WriteBytes(w, b)
}

func (b *Bytes) ReadBytes(r io.Reader) error {
	return ReadBytes(r, b)
}
