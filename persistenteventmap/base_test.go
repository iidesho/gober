package persistenteventmap

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/iidesho/gober/stream/event/store/ondisk"
)

var (
	ed              EventMap[dd]
	ctxGlobal       context.Context
	ctxGlobalCancel context.CancelFunc
	testCryptKey    = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
)

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type dd struct {
	Name   string `json:"name"`
	Extras []interface {
		Type() int
	}
	Id int `json:"id"`
}

type e1 struct {
	V1 string
}

func (e e1) Type() int {
	return 1
}

type e2 struct {
	V2 string
}

func (e e2) Type() int {
	return 2
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestPre(t *testing.T) {
	os.RemoveAll("./eventmap/testdata")
}

func TestInit(t *testing.T) {
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	store, err := ondisk.Init(STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	edt, err := Init[dd](
		store,
		"testdata",
		"1.0.0",
		cryptKeyProvider,
		func(d dd) string { return fmt.Sprintf("%d_%s", d.Id, d.Name) },
		ctxGlobal,
	)
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
			Type() int
		}{
			e1{
				V1: "var1 in e1",
			},
			e2{
				V2: "var2 in e2",
			},
		},
	}
	err := ed.Set(data)
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
