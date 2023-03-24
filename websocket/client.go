package websocket

import (
	"context"
	"errors"
	log "github.com/cantara/bragi/sbragi"
	"github.com/gobwas/ws"
	"net/url"
	"nhooyr.io/websocket"
	"reflect"
)

func Dial[T any](url *url.URL, ctx context.Context) (readerOut <-chan T, writerOut chan<- Write[T], err error) {
	log.Info("dailing websocket", "url", url.String())
	//conn, _, err := websocket.Dial(ctx, url.String(), nil)
	conn, initBuff, _, err := ws.Dial(ctx, url.String())
	if err != nil {
		//log.WithError(err).Fatal("while connecting to nerthus", "url", url.String())
		return
	}
	reader := make(chan T, BufferSize)
	writer := make(chan Write[T], BufferSize)
	go func() {
		defer func() {
			//conn.Close(websocket.StatusNormalClosure, "done")
			log.WithError(conn.Close()).Info("closing websocket")
		}()
		for write := range writer {
			err := WriteWebsocket[T](conn, write)
			if err != nil {
				log.WithError(err).Error("while writing to websocket", "path", url.String(), "type", reflect.TypeOf(write).String(), "data", write) // This could end up logging person sensitive data.
				return
			}
		}
	}()
	go func() {
		defer close(reader)
		var read T
		if initBuff != nil {
			read, err = ReadWebsocket[T](initBuff)
			if err != nil {
				log.WithError(err).Error("while reading from websocket", "type", reflect.TypeOf(read).String(), "isCloseError", errors.Is(err, websocket.CloseError{})) // This could end up logging person sensitive data.
				return
			}
			reader <- read
			ws.PutReader(initBuff)
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
				read, err = ReadWebsocket[T](conn)
				if err != nil {
					log.WithError(err).Error("while reading from websocket", "type", reflect.TypeOf(read).String(), "isCloseError", errors.Is(err, websocket.CloseError{})) // This could end up logging person sensitive data.
					return
				}
				reader <- read
			}
		}
	}()
	writerOut = writer
	readerOut = reader
	return
}
