package bcts

import (
	"io"
)

type Nil struct{}

func (n Nil) WriteBytes(w io.Writer) error {
	return nil
}

func (n *Nil) ReadBytes(r io.Reader) error {
	return nil
}
