package main

import (
	"context"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/webserver"
	"github.com/cantara/gober/websocket"
	"github.com/cantara/gober/websocket/example"
	"github.com/gin-gonic/gin"
)

func main() {
	nl, err := log.NewDebugLogger()
	if err != nil {
		log.WithError(err).Fatal("while creating new logger")
	}
	nl.SetDefault()
	serv, err := webserver.Init(4123, true)
	if err != nil {
		log.WithError(err).Fatal("while initing webserver")
		return
	}

	websocket.Serve[example.PongData](serv.API(), "/ping", nil,
		func(reader <-chan example.PongData, writer chan<- websocket.Write[example.PongData], params gin.Params, ctx context.Context) {
			defer close(writer)
			for read := range reader {
				time.Sleep(read.Sleep)
				errChan := make(chan error, 1)
				writer <- websocket.Write[example.PongData]{
					Data: read,
					Err:  errChan,
				}
				err := <-errChan
				if err != nil {
					log.WithError(err).Error("while writing pong response")
					return
				}
			}
		},
	)

	serv.Run()
}
