package bcts

import (
	"io"
)

type Reader[T any] interface {
	ReadBytes(io.Reader) error
	*T
}
type Writer interface {
	WriteBytes(io.Writer) error
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
type ComparableReadWriter[T any] interface {
	ComparableReader[T]
	ComparableWriter
}
