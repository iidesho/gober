package webserver

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/iidesho/bragi/sbragi"
)

func TestPanicRecover(t *testing.T) {
	serv, err := Init(9298, true)
	if err != nil {
		t.Fatal(err)
	}
	serv.API().Get("panic", func(c *fiber.Ctx) error {
		panic("TEST")
	})
	go serv.Run()
	resp, err := http.Get("http://localhost:9298/panic")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 500 {
		t.Fatal("panic did not result in 500")
	}
}

func TestHealth(t *testing.T) {
	serv, err := Init(9299, true)
	if err != nil {
		t.Fatal(err)
	}
	go serv.Run()
	resp, err := http.Get("http://localhost:9299/health")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatal("get did not respond successfully")
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	sbragi.Info("health", "body", string(b))
}

func BenchmarkStoreAndStream(b *testing.B) {
	l, _ := sbragi.NewLogger(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource: true,
		Level:     sbragi.LevelError,
	}))
	l.SetDefault()
	port := uint16(9299 + (b.N % 230))
	url := fmt.Sprintf("http://localhost:%d/health", port)
	serv, err := Init(port, true)
	if err != nil {
		b.Fatal(err)
	}
	go serv.Run()
	c := http.Client{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := c.Get(url)
		if err != nil {
			b.Fatal(err)
		}
		if resp.StatusCode != 200 {
			b.Fatal("get did not respond successfully")
		}
	}
}
