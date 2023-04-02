package websocket

import (
	"context"
	"errors"
	log "github.com/cantara/bragi/sbragi"
	"github.com/gobwas/ws"
	"io"
	"net/url"
	"nhooyr.io/websocket"
	"reflect"
	"sync"
	"time"
)

func Dial[T any](url *url.URL, ctx context.Context) (readerOut <-chan T, writerOut chan<- Write[T], err error) {
	log.Info("dailing websocket", "url", url.String())
	conn, initBuff, _, err := ws.Dial(ctx, url.String())
	if err != nil {
		//log.WithError(err).Fatal("while connecting to nerthus", "url", url.String())
		return
	}
	serverClosed := false
	reader := make(chan T, BufferSize)
	writer := make(chan Write[T], BufferSize)
	tick := time.Second * 20
	sucker := webSucker[T]{
		pingTimout: tick,
		pingTicker: time.NewTicker(tick),
		writeLock:  sync.Mutex{},
		conn:       conn,
	}
	go func() {
		defer func() {
			if !serverClosed {
				err = ws.WriteFrame(conn, ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "writer closed")))
				log.WithError(err).Info("writing client websocket close frame")
			}
			log.WithError(conn.Close()).Info("closing client net conn")
		}()
		for {
			select {
			case write, ok := <-writer:
				if !ok {
					return
				}
				err := sucker.Write(write)
				if err != nil {
					log.WithError(err).Error("while writing to websocket", "path", url.String(), "type", reflect.TypeOf(write).String(), "data", write) // This could end up logging person sensitive data.
					return
				}
			case <-sucker.pingTicker.C:
				err = sucker.Ping()
				if err != nil && errors.Is(err, ErrNoErrorHandled) {
					log.Debug("no ping already waiting for pong from server")
					continue
				}
				log.WithError(err).Debug("wrote ping from client")
			}
		}
	}()
	go func() {
		defer close(reader)
		var read T
		if initBuff != nil {
			/*
				read, err = ReadWebsocket[T](&inBuff{read: initBuff, write: conn}, connWriter)
				if err != nil {
					/*
						if errors.Is(err, ErrNoErrorHandled) {
							continue
						}
						if errors.Is(err, ErrNotImplemented) {
							log.WithError(err).Warning("continuing after packet is discarded")
							continue
						}
			*/ /*
					if errors.Is(err, io.EOF) {
						serverClosed = true
						log.Info("websocket is closed, client ending...")
						return
					}
					log.WithError(err).Error("while reading from websocket", "type", reflect.TypeOf(read).String(), "isCloseError", errors.Is(err, websocket.CloseError{})) // This could end up logging person sensitive data.
					return
				}
				tkr.Reset(tickD)
				reader <- read
			*/
			ws.PutReader(initBuff)
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
				//tkr.Stop()
				//read, err = ReadWebsocket[T](conn, connWriter)
				read, err = sucker.Read()
				if err != nil {
					if errors.Is(err, ErrNoErrorHandled) {
						continue
					}
					if errors.Is(err, ErrNotImplemented) {
						log.WithError(err).Warning("continuing after packet is discarded")
						continue
					}
					if errors.Is(err, io.EOF) {
						serverClosed = true
						log.Info("websocket is closed, client ending...")
						return
					}
					log.WithError(err).Error("while client reading from websocket", "type", reflect.TypeOf(read).String(), "isCloseError", errors.Is(err, websocket.CloseError{})) // This could end up logging person sensitive data.
					return
				}
				//tkr.Reset(tickD)
				reader <- read
			}
		}
	}()
	writerOut = writer
	readerOut = reader
	return
}

type inBuff struct {
	read  io.Reader
	write io.Writer
}

func (b *inBuff) Read(p []byte) (n int, err error) {
	return b.read.Read(p)
}

func (b *inBuff) Write(p []byte) (n int, err error) {
	return b.write.Write(p)
}
