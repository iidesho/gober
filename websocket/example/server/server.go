package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/webserver"
	"github.com/iidesho/gober/websocket"
	"github.com/iidesho/gober/websocket/example"
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

	serv.API().GET("/ping", func(ctx *gin.Context) {
		websocket.Serve[example.PongData](
			nil,
			func(reader <-chan example.PongData, writer chan<- websocket.Write[example.PongData], r *http.Request, ctx context.Context) {
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
		)(
			ctx.Writer,
			ctx.Request,
		)
	})

	serv.Run()
}
