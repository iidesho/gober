package main

import (
	"context"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/websocket"
	"github.com/iidesho/gober/websocket/example"
	"net/url"
	"time"
)

func main() {
	nl, err := log.NewDebugLogger()
	if err != nil {
		log.WithError(err).Fatal("while creating new logger")
	}
	nl.SetDefault()
	u, err := url.Parse("ws://localhost:4123/ping")
	if err != nil {
		log.WithError(err).Fatal("while parsing url")
	}
	reader, writer, err := websocket.Dial[example.PongData](u, context.Background())
	if err != nil {
		log.WithError(err).Fatal("while dialing websocket")
	}
	for i := 0; i < 50; i++ {
		writer <- websocket.Write[example.PongData]{
			Data: example.PongData{Data: "ping", Sleep: time.Second * 2},
		}
	}
	for i := 0; i < 50; i++ {
		log.Info("read", "msg", <-reader)
	}
}
