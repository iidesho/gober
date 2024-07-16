package bcts

import (
	"bufio"
	"io"
)

type TinyString string
type SmallString string
type String string

func (s TinyString) WriteBytes(w *bufio.Writer) error {
	return WriteTinyString(w, s)
}

func (s *TinyString) ReadBytes(r io.Reader) error {
	return ReadTinyString(r, s)
}

func (s SmallString) WriteBytes(w *bufio.Writer) error {
	return WriteSmallString(w, s)
}

func (s *SmallString) ReadBytes(r io.Reader) error {
	return ReadSmallString(r, s)
}

func (s String) WriteBytes(w *bufio.Writer) error {
	return WriteString(w, s)
}

func (s *String) ReadBytes(r io.Reader) error {
	return ReadString(r, s)
}
