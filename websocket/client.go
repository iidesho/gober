package websocket

import (
	"context"
	"errors"
	log "github.com/cantara/bragi/sbragi"
	"net/url"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
	"reflect"
)

func Dial[T any](url *url.URL, ctx context.Context) (readerOut <-chan T, writerOut chan<- Write[T], err error) {
	log.Info("dailing websocket", "url", url.String())
	conn, _, err := websocket.Dial(ctx, url.String(), nil)
	if err != nil {
		//log.WithError(err).Fatal("while connecting to nerthus", "url", url.String())
		return
	}
	reader := make(chan T, BufferSize)
	writer := make(chan Write[T], BufferSize)
	go func() {
		defer func() {
			log.WithError(conn.Close(websocket.StatusNormalClosure, "done")).Info("closing websocket")
		}()
		for write := range writer {
			func() {
				defer func() {
					if write.Err != nil {
						close(write.Err)
					}
				}()
				err = wsjson.Write(ctx, conn, write.Data)
				if err != nil {
					if write.Err != nil {
						write.Err <- err
						return
					}
					log.WithError(err).Error("while writing to websocket", "path", url.String(), "type", reflect.TypeOf(write).String(), "data", write) // This could end up logging person sensitive data.
					return
				}
			}()
		}
	}()
	go func() {
		defer close(reader)
		var read T
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err = wsjson.Read(ctx, conn, &read)
				if err != nil {
					log.WithError(err).Error("while reading from websocket", "type", reflect.TypeOf(read).String(), "isCloseError", errors.Is(err, websocket.CloseError{})) // This could end up logging person sensitive data.
					if errors.Is(err, websocket.CloseError{}) {
						return
					}
					continue
				}
				reader <- read
			}
		}
	}()
	writerOut = writer
	readerOut = reader
	return
}
