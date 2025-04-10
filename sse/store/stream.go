package store

import (
	"bufio"
	"context"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream/event/store"
)

const (
	B  = 1
	KB = B << 10
	MB = KB << 10
	GB = MB << 10
)

type streamClient struct {
	ctx       context.Context
	writeChan chan<- store.WriteEvent
	url       *url.URL
	client    *http.Client
	position  atomic.Uint64
}

func Connect(c *http.Client, endpoint *url.URL, ctx context.Context) (s *streamClient, err error) {
	s = &streamClient{
		url:      endpoint,
		client:   c,
		position: atomic.Uint64{},
		ctx:      ctx,
	}
	return
}

func (s *streamClient) Write() chan<- store.WriteEvent {
	return s.writeChan
}

func (s *streamClient) Stream(
	from store.StreamPosition,
	ctx context.Context,
) (out <-chan store.ReadEvent, err error) {
	eventChan := make(chan store.ReadEvent, 2)
	out = eventChan
	go readStream(s, eventChan, uint64(from), ctx)
	return
}

func readStream(
	s *streamClient,
	events chan<- store.ReadEvent,
	position uint64,
	ctx context.Context,
) {
	exit := false
	defer func() {
		if exit {
			close(events)
		}
	}()
	/*
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			log.WithError(fmt.Errorf("%v", r)).Error("recovering read stream", "stream", s.url)
			readStream(s, events, position, ctx)
		}()
	*/
	req, err := http.NewRequest("GET", s.url.String(), nil)
	if err != nil {
		log.WithError(err).Fatal("while opening stream file")
		return
	}

	req.Header.Set("Content-Type", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	resp, err := s.client.Do(req)
	if err != nil {
		log.WithError(err).Fatal("while opening stream file")
		return
	}

	if position == uint64(store.STREAM_END) {
		position = s.position.Load()
	}
	// readTo := uint64(0)
	reader := bufio.NewReader(resp.Body)
	go func() {
		defer resp.Body.Close()
		for {
			select {
			case <-ctx.Done():
				exit = true
				return
			case <-s.ctx.Done():
				exit = true
				return
			default:
			}
			line, err := reader.ReadString('\n')
			if log.WithError(err).Error("Connection closed") {
				break
			}
			select {
			case <-ctx.Done():
				exit = true
				return
			case <-s.ctx.Done():
				exit = true
				return
			default:
			}
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			line = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			data, err := base64.RawURLEncoding.DecodeString(line)
			if log.WithError(err).Error("decoding b64 event", "line", line) {
				break
			}
			e, err := bcts.Read[store.ReadEvent](data)
			if log.WithError(err).Error("decoding event") {
				break
			}
			position = e.Position
			// streamClient.position
			events <- *e
		}
	}()
	/*
		for readTo < position {
			// err = binary.Read(db, binary.LittleEndian, &se)
			if err != nil {
				if errors.Is(err, io.EOF) {
					time.Sleep(time.Millisecond * 250)
					continue
				}
				log.WithError(err).Fatal("while unmarshalling event from store catchup", "name", s.url)
			}
			readTo = se.Position
		}
	*/
}

func (s *streamClient) Name() string {
	return s.url.String()
}

func (s *streamClient) End() (pos uint64, err error) {
	pos = s.position.Load()
	return
}
