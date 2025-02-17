package bcts

import (
	"io"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
)

type UUID uuid.UUID

func NewUUID() UUID {
	u, err := uuid.NewV7()
	sbragi.WithError(err).Fatal("could not create uuid, this should be a hardware issue...")
	return UUID(u)
}

func (u UUID) WriteBytes(w io.Writer) error {
	return WriteStaticBytes(w, u[:])
}

func (u *UUID) ReadBytes(r io.Reader) error {
	if u == nil {
		u = &UUID{}
	}
	return ReadStaticBytes(r, u[:])
}
