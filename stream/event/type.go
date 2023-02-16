package event

import (
	"fmt"
)

type Type string

const (
	Create  Type = "create"
	Update  Type = "update"
	Delete  Type = "delete"
	Invalid Type = "invalid"
)

func AllTypes() []Type {
	return []Type{Create, Update, Delete}
}

func TypeFromString(s string) Type {
	switch s {
	case string(Create):
		return Create
	case string(Update):
		return Update
	case string(Delete):
		return Delete
	}
	return Invalid
}

var InvalidTypeError = fmt.Errorf("event type is invalid")
