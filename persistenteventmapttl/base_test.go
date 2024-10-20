package persistenteventmapttl

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/iidesho/gober/stream"
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
	Name string `json:"name"`
	Id   int    `json:"id"`
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
		stream.StaticProvider(testCryptKey),
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
	}
	err := ed.Set(data)
	if err != nil {
		t.Error(err)
		return
	}
	return
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
