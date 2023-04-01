package websocket

import (
	"context"
	"errors"
	log "github.com/cantara/bragi/sbragi"
	"github.com/gin-gonic/gin"
	"github.com/gobwas/ws"
	jsoniter "github.com/json-iterator/go"
	"io"
	"net"
	"reflect"
)

var json = jsoniter.ConfigDefault

var BufferSize = 10

func Serve[T any](r *gin.RouterGroup, path string, acceptFunc func(c *gin.Context) bool, wsfunc WSHandler[T]) {
	r.GET(path, func(c *gin.Context) {
		if acceptFunc != nil && !acceptFunc(c) {
			return //Could be smart to have some check of weather or not the statuscode code has been set.
		}
		conn, _, _, err := ws.UpgradeHTTP(c.Request, c.Writer)
		if err != nil {
			log.WithError(err).Fatal("while accepting websocket", "request", c.Request)
		}
		clientClosed := false
		reader := make(chan T, BufferSize)
		writer := make(chan Write[T], BufferSize)
		go func() {
			defer func() {
				if !clientClosed {
					err = ws.WriteFrame(conn, ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "writer closed")))
					log.WithError(err).Info("writing server websocket close frame")
				}
				log.WithError(conn.Close()).Info("closing server net conn")
			}()
			for write := range writer {
				err := WriteWebsocket[T](conn, write)
				if err != nil {
					log.WithError(err).Error("while writing to websocket", "path", path, "request", c.Request, "type", reflect.TypeOf(write).String(), "data", write) // This could end up logging person sensitive data.
					return
				}
			}
		}()
		go func() {
			defer close(reader)
			var read T
			var err error
			for {
				select {
				case <-c.Request.Context().Done():
					return
				default:
					read, err = ReadWebsocket[T](conn)
					if err != nil {
						if errors.Is(err, ErrNotImplemented) {
							continue
						}
						if errors.Is(err, io.EOF) {
							clientClosed = true
							log.Info("websocket is closed, server ending...") //This works, but gave a wrong impression, changed slightly
							return
						}
						log.WithError(err).Error("while server reading from websocket", "path", path, "request", c.Request, "type", reflect.TypeOf(read).String()) // This could end up logging person sensitive data.
						return
					}
					reader <- read
				}
			}
		}()
		wsfunc(reader, writer, c.Params, c.Request.Context())
	})
}

func ReadWebsocket[T any](conn io.ReadWriter) (out T, err error) {
	header, err := ws.ReadHeader(conn)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = io.EOF
			return
		}
		return
	}
	if header.OpCode == ws.OpClose {
		err = io.EOF
		return
	}
	if header.OpCode == ws.OpPing {
		log.Info("ping received, ponging...")
		//Could also use ws.NewPingFrame(body)
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
		_, err = io.CopyN(conn, conn, header.Length)
		return
	}

	/*
		1. Should verify against outstanding ping TODO
		2. Should ignore if no outstanding ping
	*/
	if header.OpCode == ws.OpPong {
		err = ErrNotImplemented
		return
	}

	if header.OpCode == ws.OpContinuation {
		err = ErrNotImplemented
		return
	}

	if header.OpCode == ws.OpBinary {
		err = ErrNotImplemented
		return
	}

	payload := make([]byte, header.Length)
	_, err = io.ReadFull(conn, payload) //Could be an idea to change this to ReadAll to not have EOF errors. Or silence them ourselves
	/*
		total, err := conn.Read(payload)
		var n int
		for err == nil && total < int(header.Length) {
			n, err = conn.Read(payload[total:])
			total += n
		}
	*/
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = io.EOF
			return
		}
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

var ErrNotImplemented = errors.New("operation not implemented")
