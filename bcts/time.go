package bcts

import (
	"bufio"
	"io"
	"time"
)

type Time time.Time

func (t Time) WriteBytes(w *bufio.Writer) error {
	return WriteTime(w, t.Time())
}

func (t *Time) ReadBytes(r io.Reader) error {
	var tmp time.Time
	err := ReadTime(r, &tmp)
	if err != nil {
		return err
	}
	return nil
}

func (t Time) Time() time.Time {
	return time.Time(t)
}
