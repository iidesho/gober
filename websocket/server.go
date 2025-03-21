package websocket

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/gobwas/ws"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/iidesho/bragi/sbragi"
	jsoniter "github.com/json-iterator/go"
)

var (
	log  = sbragi.WithLocalScope(sbragi.LevelError)
	json = jsoniter.ConfigDefault
)

var BufferSize = 100

func ServeFiber[T any](
	r fiber.Router,
	path string,
	acceptFunc func(c *fiber.Ctx) error,
	wsfunc WSHandler[T],
) {
	r.Get(path, func(c *fiber.Ctx) error {
		if acceptFunc != nil {
			if err := acceptFunc(c); err != nil {
				return errors.Join(err, errors.New(
					"unacceptable",
				)) // Could be smart to have some check of weather or not the statuscode code has been set.
			}
		}
		ctx, cancel := context.WithCancel(c.Context())
		return websocket.New(func(c *websocket.Conn) {
			defer cancel()
			clientClosed := false
			reader := make(chan T, BufferSize)
			writer := make(chan Write[T], BufferSize)
			// tick := time.Second * 50
			tick := time.Millisecond * 50
			sucker := webSucker[T]{
				pingTimout: tick,
				pingTicker: time.NewTicker(tick),
				writeLock:  sync.Mutex{},
				conn:       c.NetConn(),
			}
			/*
				connWriter := make(chan []byte, 1)
				go func() {
					defer func() {
						if !clientClosed {
							err = ws.WriteFrame(conn, ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "writer closed")))
							log.WithError(err).Info("writing client websocket close frame")
						}
						log.WithError(conn.Close()).Info("closing client net conn")
					}()
					tickD := time.Second * 50
					tkr := time.NewTicker(tickD)
					defer tkr.Stop()
					for {
						select {
						case write, ok := <-connWriter:
							if !ok {
								return
							}
							n, err := conn.Write(write)
							total := n
							for err == nil && total < len(write) {
								n, err = conn.Write(write[total:])
								total += n
							}
							if err != nil {
								log.WithError(err).Error("while writing to websocket", "path", path, "type", reflect.TypeOf(write).String(), "data", write) // This could end up logging person sensitive data.
								return
							}
							tkr.Reset(tickD)
						case <-tkr.C:
							connWriter <- ws.CompiledPing
							log.WithError(err).Info("wrote ping from server")
						}
					}
				}()
			*/
			go func() {
				defer close(reader)
				var read T
				var err error
				for {
					select {
					case <-ctx.Done():
						return
					default:
						// read, err = ReadWebsocket[T](conn, connWriter)
						read, err = sucker.Read()
						if err != nil {
							if errors.Is(err, ErrNoErrorHandled) {
								continue
							}
							if errors.Is(err, ErrNotImplemented) {
								log.WithError(err).Warning("continuing after packet is discarded")
								continue
							}
							if errors.Is(err, net.ErrClosed) {
								clientClosed = true
								cancel()
								return
							}
							if errors.Is(err, io.EOF) {
								clientClosed = true
								cancel()
								log.Info(
									"websocket is closed, server closing...",
								) // This works, but gave a wrong impression, changed slightly
								return
							}
							log.WithError(err).
								Error("while server reading from websocket", "request", r, "type", reflect.TypeOf(read).String())

								// This could end up logging person sensitive data.
							return
						}
						reader <- read
					}
				}
			}()
			go wsfunc(reader, writer, nil, ctx)
			var err error
			defer func() {
				if !clientClosed {
					err = ws.WriteFrame(
						sucker.conn,
						ws.NewCloseFrame(
							ws.NewCloseFrameBody(ws.StatusNormalClosure, "writer closed"),
						),
					)
					log.WithError(err).Info("writing client websocket close frame")
				}
				log.WithError(sucker.conn.Close()).Info("closing client net conn")
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case write, ok := <-writer:
					if !ok {
						return
					}
					// err := WriteWebsocket[T](connWriter, write)
					err := sucker.Write(write)
					if err != nil {
						if errors.Is(err, net.ErrClosed) {
							clientClosed = true
							cancel()
							return
						}
						log.WithError(err).
							Error("while writing to websocket", "request", r, "type", reflect.TypeOf(write).String())

							// This could end up logging person sensitive data.
						return
					}
				case <-sucker.pingTicker.C:
					err = sucker.Ping()
					if err != nil {
						if errors.Is(err, ErrNoErrorHandled) {
							// log.Debug("no ping already waiting for pong from client")
							continue
						}
						if errors.Is(err, net.ErrClosed) {
							clientClosed = true
							cancel()
							return
						}
					}
					log.WithError(err).Debug("wrote ping from server")
				}
			}
		})(c)
	})
}

func Serve[T any](acceptFunc func(r *http.Request) bool, wsfunc WSHandler[T]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if acceptFunc != nil && !acceptFunc(r) {
			return // Could be smart to have some check of weather or not the statuscode code has been set.
		}
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			log.WithError(err).Fatal("while accepting websocket", "request", r)
		}
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		clientClosed := false
		reader := make(chan T, BufferSize)
		writer := make(chan Write[T], BufferSize)
		tick := time.Second * 50
		sucker := webSucker[T]{
			pingTimout: tick,
			pingTicker: time.NewTicker(tick),
			writeLock:  sync.Mutex{},
			conn:       conn,
		}
		/*
			connWriter := make(chan []byte, 1)
			go func() {
				defer func() {
					if !clientClosed {
						err = ws.WriteFrame(conn, ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "writer closed")))
						log.WithError(err).Info("writing client websocket close frame")
					}
					log.WithError(conn.Close()).Info("closing client net conn")
				}()
				tickD := time.Second * 50
				tkr := time.NewTicker(tickD)
				defer tkr.Stop()
				for {
					select {
					case write, ok := <-connWriter:
						if !ok {
							return
						}
						n, err := conn.Write(write)
						total := n
						for err == nil && total < len(write) {
							n, err = conn.Write(write[total:])
							total += n
						}
						if err != nil {
							log.WithError(err).Error("while writing to websocket", "path", path, "type", reflect.TypeOf(write).String(), "data", write) // This could end up logging person sensitive data.
							return
						}
						tkr.Reset(tickD)
					case <-tkr.C:
						connWriter <- ws.CompiledPing
						log.WithError(err).Info("wrote ping from server")
					}
				}
			}()
		*/
		go func() {
			defer func() {
				if !clientClosed {
					err = ws.WriteFrame(
						conn,
						ws.NewCloseFrame(
							ws.NewCloseFrameBody(ws.StatusNormalClosure, "writer closed"),
						),
					)
					log.WithError(err).Info("writing client websocket close frame")
				}
				log.WithError(conn.Close()).Info("closing client net conn")
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case write, ok := <-writer:
					if !ok {
						return
					}
					// err := WriteWebsocket[T](connWriter, write)
					err := sucker.Write(write)
					if err != nil {
						if errors.Is(err, net.ErrClosed) {
							clientClosed = true
							cancel()
							return
						}
						log.WithError(err).
							Error("while writing to websocket", "request", r, "type", reflect.TypeOf(write).String())

							// This could end up logging person sensitive data.
						return
					}
				case <-sucker.pingTicker.C:
					err = sucker.Ping()
					if err != nil {
						if errors.Is(err, ErrNoErrorHandled) {
							// log.Debug("no ping already waiting for pong from client")
							continue
						}
						if errors.Is(err, net.ErrClosed) {
							clientClosed = true
							cancel()
							return
						}
					}
					log.WithError(err).Debug("wrote ping from server")
				}
			}
		}()
		go func() {
			defer close(reader)
			var read T
			var err error
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// read, err = ReadWebsocket[T](conn, connWriter)
					read, err = sucker.Read()
					if err != nil {
						if errors.Is(err, ErrNoErrorHandled) {
							continue
						}
						if errors.Is(err, ErrNotImplemented) {
							log.WithError(err).Warning("continuing after packet is discarded")
							continue
						}
						if errors.Is(err, net.ErrClosed) {
							clientClosed = true
							cancel()
							return
						}
						if errors.Is(err, io.EOF) {
							clientClosed = true
							cancel()
							log.Info(
								"websocket is closed, server closing...",
							) // This works, but gave a wrong impression, changed slightly
							return
						}
						log.WithError(err).
							Error("while server reading from websocket", "request", r, "type", reflect.TypeOf(read).String())

							// This could end up logging person sensitive data.
						return
					}
					reader <- read
				}
			}
		}()
		wsfunc(reader, writer, nil, ctx)
	}
}

type webSucker[T any] struct {
	conn       net.Conn
	pingTicker *time.Ticker
	pingTimout time.Duration
	pingLock   sync.Mutex
	writeLock  sync.Mutex
}

func (sucker *webSucker[T]) Ping() (err error) {
	if !sucker.pingLock.TryLock() {
		return ErrNoErrorHandled
	}
	return sucker.WriteConn(ws.CompiledPing)
}

func (sucker *webSucker[T]) WriteConn(write []byte) (err error) {
	defer sucker.pingTicker.Reset(sucker.pingTimout)
	sucker.writeLock.Lock()
	defer sucker.writeLock.Unlock()
	var n int
	n, err = sucker.conn.Write(write)
	total := n
	for err == nil && total < len(write) {
		n, err = sucker.conn.Write(write[total:])
		total += n
	}
	return
}

func (sucker *webSucker[T]) Write(write Write[T]) (err error) {
	defer func() {
		if write.Err != nil {
			close(write.Err)
		}
	}()
	var frame []byte
	var x interface{} = write.Data
	var ok bool
	if frame, ok = x.([]byte); ok {
		sbragi.Trace("wrote package", "n", len(frame))
		frame, err = ws.CompileFrame(ws.NewBinaryFrame(frame))
		if err != nil {
			return err
		}
	} else {
		payload, err := json.Marshal(write.Data)
		if err != nil {
			if write.Err != nil {
				write.Err <- err
			}
			return err
		}
		frame, err = ws.CompileFrame(ws.NewTextFrame(payload))
		if err != nil {
			return err
		}
		sbragi.Trace("wrote package", "n", len(payload))
	}
	err = sucker.WriteConn(frame)
	/*
		err = sucker.WriteConn(append(websocketHeaderBytes(ws.Header{
			Fin:    true,
			Rsv:    0,
			OpCode: ws.OpText,
			Masked: false,
			Mask:   [4]byte{},
			Length: int64(len(payload)),
		}), payload...))
	*/if err != nil {
		if write.Err != nil {
			write.Err <- err
		}
		return err
	}
	return
}

func (sucker *webSucker[T]) Read() (out T, err error) {
	// defer sucker.pingTicker.Reset(sucker.pingTimout)
	header, err := ws.ReadHeader(sucker.conn)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = io.EOF
			return
		}
		return
	}
	log.Trace("packet received", "type", packetTypeToString(header.OpCode))
	sucker.pingTicker.Stop()
	defer sucker.pingTicker.Reset(sucker.pingTimout)
	if header.OpCode == ws.OpClose {
		err = io.EOF
		return
	}
	if header.OpCode == ws.OpPing {
		log.Debug("ping received, ponging...")
		payload := make([]byte, header.Length)
		_, err = io.ReadFull(sucker.conn, payload)
		if err != nil {
			return
		}
		/*
			var frame []byte
			frame, err = ws.CompileFrame(ws.NewPongFrame(payload))
			if err != nil {
				return
			}
		*/
		err = sucker.WriteConn(ws.CompiledPong)
		/*
			err = sucker.WriteConn(append(websocketHeaderBytes(ws.Header{
				Fin:    true,
				Rsv:    0,
				OpCode: ws.OpPong,
				Masked: false,
				Mask:   [4]byte{},
				Length: header.Length,
			}), payload...))
		*/
		log.WithError(err).Trace("while ponging")
		err = ErrNoErrorHandled
		return
	}

	/*
		1. Should verify against outstanding ping TODO
		2. Should ignore if no outstanding ping
	*/
	if header.OpCode == ws.OpPong {
		log.Debug("pong received")
		sucker.pingLock.Unlock()
		if header.Length == 0 {
			err = ErrNoErrorHandled
			return
		}
		_, err = io.CopyN(io.Discard, sucker.conn, header.Length)
		err = ErrNoErrorHandled
		return
	}

	if header.OpCode == ws.OpContinuation {
		_, err = io.CopyN(io.Discard, sucker.conn, header.Length)
		sbragi.Error("continuation is not implemented")
		err = ErrNotImplemented
		return
	}

	if header.OpCode == ws.OpBinary {
		payload := make([]byte, header.Length)
		n, err := io.ReadFull(sucker.conn, payload)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				err = io.EOF
				return out, err
			}
			return out, err
		}
		sbragi.Trace("read package", "n", n)

		// p := unsafe.Pointer(&out) // Or unsafe.Pointer(&arr[0])
		// data := unsafe.Slice((*byte)(p), header.Length)
		/*
			unsafeOut := (*reflect.SliceHeader)(unsafe.Pointer(&out))
			unsafeOut.Data = uintptr(unsafe.Pointer(&payload))
			unsafeOut.Len = n
			unsafeOut.Cap = n
		*/
		out = *(*T)(unsafe.Pointer(&payload))

		/*
			if n < 30 {
				sbragi.Info(
					"data",
					"read",
					fmt.Sprintf("%d", payload),
					"converted",
					fmt.Sprintf("%v", out),
				)
			}
		*/

		runtime.KeepAlive(payload)

		/*
			sbragi.Error("binary is not implemented")
			_, err = io.CopyN(io.Discard, sucker.conn, header.Length)
			err = ErrNotImplemented
		*/
		return out, nil
	}

	payload := make([]byte, header.Length)
	n, err := io.ReadFull(sucker.conn, payload)
	if err != nil {
		if errors.Is(err, net.ErrClosed) {
			err = io.EOF
			return
		}
		return
	}
	sbragi.Trace("read package", "n", n)
	if header.Masked {
		ws.Cipher(payload, header.Mask, 0)
	}
	err = json.Unmarshal(payload, &out)
	return
}

func websocketHeaderBytes(h ws.Header) []byte {
	bts := make([]byte, ws.MaxHeaderSize)

	if h.Fin {
		bts[0] |= bit0
	}
	bts[0] |= h.Rsv << 4
	bts[0] |= byte(h.OpCode)

	var n int
	switch {
	case h.Length <= len7:
		bts[1] = byte(h.Length)
		n = 2

	case h.Length <= len16:
		bts[1] = 126
		binary.BigEndian.PutUint16(bts[2:4], uint16(h.Length))
		n = 4

	case h.Length <= len64:
		bts[1] = 127
		binary.BigEndian.PutUint64(bts[2:10], uint64(h.Length))
		n = 10

	default:
		log.WithError(ws.ErrHeaderLengthUnexpected).Fatal("while creating websocket header bytes")
	}

	if h.Masked {
		bts[1] |= bit0
		n += copy(bts[n:], h.Mask[:])
	}
	return bts[:n]
}

type WSHandler[T any] func(<-chan T, chan<- Write[T], *http.Request, context.Context)

var (
	ErrNotImplemented = errors.New("operation not implemented")
	ErrNoErrorHandled = errors.New("handled")
)

const (
	bit0 = 0x80

	len7  = int64(125)
	len16 = int64(^(uint16(0)))
	len64 = int64(^(uint64(0)) >> 1)
)

func packetTypeToString(code ws.OpCode) string {
	switch code {
	case ws.OpText:
		return "text"
	case ws.OpBinary:
		return "binary"
	case ws.OpClose:
		return "close"
	case ws.OpPing:
		return "ping"
	case ws.OpPong:
		return "pong"
	case ws.OpContinuation:
		return "continuation"
	}
	return ""
}
