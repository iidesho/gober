package websocket

import (
	"context"
	"errors"
	log "github.com/cantara/bragi/sbragi"
	"github.com/gin-gonic/gin"
	"github.com/gobwas/ws"
	jsoniter "github.com/json-iterator/go"
	"io"
	"reflect"
)

var json = jsoniter.ConfigDefault

var BufferSize = 10

func Serve[T any](r *gin.RouterGroup, path string, acceptFunc func(c *gin.Context) bool, wsfunc WSHandler[T]) {
	r.GET(path, func(c *gin.Context) {
		if acceptFunc != nil && !acceptFunc(c) {
			return //Could be smart to have some check of weather or not the statuscode code has been set.
		}
		//conn, err := websocket.Accept(c.Writer, c.Request, nil)
		conn, _, _, err := ws.UpgradeHTTP(c.Request, c.Writer)
		if err != nil {
			log.WithError(err).Fatal("while accepting websocket", "request", c.Request)
		}
		defer func() {
			err = conn.Close()
			log.WithError(err).Info("closing websocket")
		}() //Could be smart to do something here to fix / tell people of errors.
		reader := make(chan T, BufferSize)
		writer := make(chan Write[T], BufferSize)
		go func() {
			for write := range writer {
				err := WriteWebsocket[T](conn, write)
				if err != nil {
					if errors.Is(err, io.EOF) {
						log.Info("client closed websocket, closing...")
						return
					}
					log.WithError(err).Error("while writing to websocket", "path", path, "request", c.Request, "type", reflect.TypeOf(write).String(), "data", write) // This could end up logging person sensitive data.
					return
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
					read, err = ReadWebsocket[T](conn)
					if err != nil {
						if errors.Is(err, io.EOF) {
							log.Info("client closed websocket, closing...")
							return
						}
						log.WithError(err).Error("while reading from websocket", "path", path, "request", c.Request, "type", reflect.TypeOf(read).String()) // This could end up logging person sensitive data.
						return
					}
					reader <- read
				}
			}
		}()
		wsfunc(reader, writer, c.Params, c.Request.Context())
	})
}

type ReadWriter interface {
	io.Reader
	io.Writer
}

func ReadWebsocket[T any](conn ReadWriter) (out T, err error) {
	header, err := ws.ReadHeader(conn)
	if err != nil {
		return
	}
	if header.OpCode == ws.OpClose {
		err = io.EOF
		return
	}
	if header.OpCode == ws.OpPing {
		log.Info("ping received, ponging...")
		err = ws.WriteHeader(conn, ws.Header{
			Fin:    true,
			Rsv:    0,
			OpCode: ws.OpPong,
			Masked: false,
			Mask:   [4]byte{},
			Length: header.Length,
		})
		if err != nil {
			return
		}
		_, err = io.Copy(conn, conn)
		return
	}

	payload := make([]byte, header.Length)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return
	}
	if header.Masked {
		ws.Cipher(payload, header.Mask, 0)
	}
	err = json.Unmarshal(payload, &out)
	return
}

func WriteWebsocket[T any](conn io.Writer, write Write[T]) error {
	defer func() {
		if write.Err != nil {
			close(write.Err)
		}
	}()
	payload, err := json.Marshal(write.Data)
	if err != nil {
		if write.Err != nil {
			write.Err <- err
		}
		return err
	}
	err = ws.WriteHeader(conn, ws.Header{
		Fin:    true,
		Rsv:    0,
		OpCode: ws.OpText,
		Masked: false,
		Mask:   [4]byte{},
		Length: int64(len(payload)),
	})
	if err != nil {
		if write.Err != nil {
			write.Err <- err
		}
		return err
	}
	_, err = conn.Write(payload)
	if err != nil {
		if write.Err != nil {
			write.Err <- err
		}
		return err
	}
	return nil
}

type WSHandler[T any] func(<-chan T, chan<- Write[T], gin.Params, context.Context)
