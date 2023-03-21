package websocket

import (
	"context"
	"errors"
	log "github.com/cantara/bragi/sbragi"
	"github.com/gin-gonic/gin"
	"io"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
	"reflect"
)

var BufferSize = 10

func Serve[T any](r *gin.RouterGroup, path string, acceptFunc func(c *gin.Context) bool, wsfunc WSHandler[T]) {
	r.GET(path, func(c *gin.Context) {
		if acceptFunc != nil && !acceptFunc(c) {
			return //Could be smart to have some check of weather or not the statuscode code has been set.
		}
		conn, err := websocket.Accept(c.Writer, c.Request, nil)
		if err != nil {
			log.WithError(err).Fatal("while accepting websocket", "request", c.Request)
		}
		closed := false
		defer func() {
			if closed {
				return
			}
			err = conn.Close(websocket.StatusNormalClosure, "")
			log.WithError(err).Info("closing websocket")
		}() //Could be smart to do something here to fix / tell people of errors.
		reader := make(chan T, BufferSize)
		writer := make(chan Write[T], BufferSize)
		go func() {
			for write := range writer {
				err = wsjson.Write(c.Request.Context(), conn, write.Data)
				if err != nil {
					if write.Err != nil {
						write.Err <- err
						close(write.Err)
						continue
					}
					log.WithError(err).Error("while writing to websocket", "path", path, "request", c.Request, "type", reflect.TypeOf(write).String(), "data", write) // This could end up logging person sensitive data.
					continue
				}
			}
		}()
		go func() {
			defer close(reader)
			var read T
			for {
				select {
				case <-c.Request.Context().Done():
					return
				default:
					err = wsjson.Read(c.Request.Context(), conn, &read)
					if err != nil {
						log.WithError(err).Error("while reading from websocket", "path", path, "request", c.Request, "type", reflect.TypeOf(read).String()) // This could end up logging person sensitive data.
						if errors.Is(err, io.EOF) {                                                                                                         //, websocket.CloseError{}) {
							return
						}
						return //continue
					}
					reader <- read
				}
			}
		}()
		wsfunc(reader, writer, c.Params, c.Request.Context())
	})
}

type WSHandler[T any] func(<-chan T, chan<- Write[T], gin.Params, context.Context)
