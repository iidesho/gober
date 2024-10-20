package webserver

import (
	"net/http"
	"testing"

	"github.com/gofiber/fiber/v2"
)

func TestPanicRecover(t *testing.T) {
	serv, err := Init(9299, true)
	if err != nil {
		t.Fatal(err)
	}
	serv.API().Get("panic", func(c *fiber.Ctx) error {
		panic("TEST")
	})
	go serv.Run()
	resp, err := http.Get("http://localhost:9299/panic")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 500 {
		t.Fatal("panic did not result in 500")
	}
}
