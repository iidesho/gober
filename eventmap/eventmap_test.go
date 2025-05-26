package eventmap

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream/event/store/ondisk"

	"github.com/google/uuid"
)

var (
	ed              EventMap[dd, *dd]
	ctxGlobal       context.Context
	ctxGlobalCancel context.CancelFunc
	testCryptKey    = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
)

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.New().String()

type dd struct {
	Name   string `json:"name"`
	Extras []interface {
		Type() uint8
		bcts.Writer
	}
	Id int64 `json:"id"`
}

type e1 struct {
	V1 string
}

func (e e1) Type() uint8 {
	return 1
}

func (e e1) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteTinyString(w, e.V1)
	if err != nil {
		return
	}
	return nil
}

func (e *e1) ReadBytes(r io.Reader) (err error) {
	err = bcts.ReadTinyString(r, &e.V1)
	if err != nil {
		return
	}
	return nil
}

type e2 struct {
	V2 string
}

func (e e2) Type() uint8 {
	return 2
}

func (e e2) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteTinyString(w, e.V2)
	if err != nil {
		return
	}
	return nil
}

func (e *e2) ReadBytes(r io.Reader) (err error) {
	err = bcts.ReadTinyString(r, &e.V2)
	if err != nil {
		return
	}
	return nil
}

func (s dd) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteInt64(w, s.Id)
	if err != nil {
		return
	}
	err = bcts.WriteTinyString(w, s.Name)
	if err != nil {
		return
	}
	err = bcts.WriteUInt32(w, uint32(len(s.Extras)))
	if err != nil {
		return err
	}
	for _, e := range s.Extras {
		err = bcts.WriteUInt8(w, e.Type())
		if err != nil {
			return err
		}
		err = e.WriteBytes(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *dd) ReadBytes(r io.Reader) (err error) {
	err = bcts.ReadInt64(r, &s.Id)
	if err != nil {
		return
	}
	err = bcts.ReadTinyString(r, &s.Name)
	if err != nil {
		return
	}
	var l uint32
	err = bcts.ReadUInt32(r, &l)
	if err != nil {
		return err
	}
	s.Extras = make([]interface {
		Type() uint8
		bcts.Writer
	}, l)
	for i := range l {
		var t uint8
		err = bcts.ReadUInt8(r, &t)
		if err != nil {
			return err
		}
		switch t {
		case 1:
			var v e1
			err = v.ReadBytes(r)
			if err != nil {
				return err
			}
			s.Extras[i] = v
		case 2:
			var v e2
			err = v.ReadBytes(r)
			if err != nil {
				return err
			}
			s.Extras[i] = v
		}
	}
	return nil
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestInit(t *testing.T) {
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	store, err := ondisk.Init(STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	edt, err := Init[dd](store, "setandwait", "1.0.0", cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ed = edt
}

func TestStore(t *testing.T) {
	data := dd{
		Id:   1,
		Name: "test",
		Extras: []interface {
			Type() uint8
			bcts.Writer
		}{
			e1{
				V1: "var1 in e1",
			},
			e2{
				V2: "var2 in e2",
			},
		},
	}
	err := ed.Set("1_test", &data)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestGet(t *testing.T) {
	data, err := ed.Get("1_test")
	fmt.Println(data)
	if err != nil {
		t.Error(err)
		return
	}
	if data.Id != 1 {
		t.Error(fmt.Errorf("missmatch data id"))
		return
	}
	if data.Name != "test" {
		t.Error(fmt.Errorf("missmatch data name"))
		return
	}
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}
