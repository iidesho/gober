package bcts

import (
	"bufio"
	"io"
)

type Reader[T any] interface {
	ReadBytes(io.Reader) error
	*T
}
type Writer interface {
	WriteBytes(*bufio.Writer) error
}
type ReadWriter[T any] interface {
	Reader[T]
	Writer
}

type ComparableWriter interface {
	comparable
	Writer
}

type ComparableReader[T any] interface {
	comparable
	Reader[T]
}
