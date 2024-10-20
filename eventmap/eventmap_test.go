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
	Name string `json:"name"`
	Id   int64  `json:"id"`
}

func (s dd) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteInt64(w, s.Id)
	if err != nil {
		return
	}
	return bcts.WriteTinyString(w, s.Name)
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
