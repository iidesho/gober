package event

import (
	"fmt"
	"io"

	"github.com/iidesho/gober/bcts"
)

type Type string

func (t Type) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0)) //Version
	if err != nil {
		return
	}
	err = bcts.WriteString(w, t)
	if err != nil {
		return
	}
	return nil
}

func (t *Type) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid Type version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadString(r, t)
	if err != nil {
		return
	}
	return nil
}

const (
	Created Type = "created"
	Updated Type = "updated"
	Deleted Type = "deleted"
	Invalid Type = "invalid"
)

func AllTypes() []Type {
	return []Type{Created, Updated, Deleted}
}

func TypeFromString(s string) Type {
	switch s {
	case string(Created):
		return Created
	case string(Updated):
		return Updated
	case string(Deleted):
		return Deleted
	}
	return Invalid
}

var ErrInvalidType = fmt.Errorf("event type is invalid")
