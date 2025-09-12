package ondisk

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/metrics"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	B  = 1
	KB = B << 10
	MB = KB << 10
	GB = MB << 10
)

var (
	log            = sbragi.WithLocalScope(sbragi.LevelInfo)
	writeCount     *prometheus.CounterVec
	writeTimeTotal *prometheus.CounterVec
	readCount      *prometheus.CounterVec
	readTimeTotal  *prometheus.CounterVec
)

type storeEvent struct {
	Created  time.Time
	Event    store.Event
	Position uint64
	// Version  uint8 // Do not need this to be in memory as the version is only relevant for on disk format
}

func (e storeEvent) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0))
	if err != nil {
		return
	}
	err = bcts.WriteTime(w, e.Created)
	if err != nil {
		return
	}
	err = bcts.WriteSmallString(w, e.Event.Type)
	if err != nil {
		return
	}
	err = bcts.WriteBytes(w, e.Event.Data)
	if err != nil {
		return
	}
	err = bcts.WriteBytes(w, e.Event.Metadata)
	if err != nil {
		return
	}
	err = bcts.WriteStaticBytes(w, e.Event.Id.Bytes())
	if err != nil {
		return
	}
	err = bcts.WriteUInt64(w, e.Position)
	if err != nil {
		return
	}
	return nil
}

func (e *storeEvent) ReadBytes(r io.Reader) (err error) {
	var v uint8
	err = bcts.ReadUInt8(r, &v)
	if err != nil {
		return
	}
	if v != 0 {
		return fmt.Errorf("invalid stored event version, %s=%d, %s=%d", "expected", 0, "got", v)
	}
	err = bcts.ReadTime(r, &e.Created)
	if err != nil {
		return
	}
	err = bcts.ReadSmallString(r, &e.Event.Type)
	if err != nil {
		return
	}
	err = bcts.ReadBytes(r, &e.Event.Data)
	if err != nil {
		return
	}
	err = bcts.ReadBytes(r, &e.Event.Metadata)
	if err != nil {
		return
	}
	err = bcts.ReadStaticBytes(r, e.Event.Id[:])
	if err != nil {
		return
	}
	return bcts.ReadUInt64(r, &e.Position)
}

// stream Need to add a way to not store multiple events with the same id in the same stream.
type stream struct {
	db  *os.File
	len *atomic.Uint64
	// dbLock   *sync.Mutex
	newData *sync.Cond
}

type Stream struct {
	ctx       context.Context
	writeChan chan<- store.WriteEvent
	data      stream
	name      string
}

func Init(name string, ctx context.Context) (s *Stream, err error) {
	writeChan := make(chan store.WriteEvent)
	os.Mkdir("streams", 0750)
	f, err := os.OpenFile(fmt.Sprintf("streams/%s", name), os.O_CREATE|os.O_RDONLY, 0640)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := f
	// j := json.NewDecoder(f)
	var se storeEvent
	p := uint64(0)
	for err = se.ReadBytes(r); err == nil; err = se.ReadBytes(r) {
		if p < se.Position {
			p = se.Position
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	f, err = os.OpenFile(fmt.Sprintf("streams/%s", name), os.O_WRONLY|os.O_APPEND|os.O_SYNC, 0640)
	if err != nil {
		return nil, err
	}
	s = &Stream{
		data: stream{
			db:  f,
			len: &atomic.Uint64{},
			// dbLock:  &sync.Mutex{},
			newData: sync.NewCond(&sync.Mutex{}),
		},
		name:      name,
		writeChan: writeChan,
		ctx:       ctx,
	}
	s.data.len.Store(p)
	if metrics.Registry != nil && writeCount == nil {
		writeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ondisk_event_write_count",
			Help: "ondisk event write count",
		}, []string{"stream"})
		err = metrics.Registry.Register(writeCount)
		if err != nil {
			return nil, err
		}
		writeTimeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ondisk_event_write_time_total",
			Help: "ondisk event write time total",
		}, []string{"stream"})
		err = metrics.Registry.Register(writeTimeTotal)
		if err != nil {
			return nil, err
		}
		readCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ondisk_event_read_count",
			Help: "ondisk event read count",
		}, []string{"stream"})
		err = metrics.Registry.Register(readCount)
		if err != nil {
			return nil, err
		}
		readTimeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ondisk_event_read_time_total",
			Help: "ondisk event read time total",
		}, []string{"stream"})
		err = metrics.Registry.Register(readTimeTotal)
		if err != nil {
			return nil, err
		}
	}
	s.data.len.Store(p)
	go writeStream(s, writeChan)
	return s, nil
}

func writeStream(s *Stream, writes <-chan store.WriteEvent) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		log.WithError(fmt.Errorf("%v", r)).Error("recovering write stream", "stream", s.name)
		writeStream(s, writes)
	}()
	w := bufio.NewWriter(s.data.db)
	for {
		select {
		case <-s.ctx.Done():
			return
		case e := <-writes:
			func() {
				if writeCount != nil {
					start := time.Now()
					defer func() {
						writeCount.WithLabelValues(s.name).Inc()
						writeTimeTotal.WithLabelValues(s.name).
							Add(float64(time.Since(start).Microseconds()))
					}()
				}
				defer func() {
					if e.Status != nil {
						close(e.Status)
					}
				}()
				nextPoss := s.data.len.Load() + 1
				se := storeEvent{
					Event: e.Event,
					// Position: s.data.len.Add(1),
					Position: nextPoss,
					Created:  time.Now(),
				}
				err := se.WriteBytes(w)
				//_, err := w.Write(se.Bytes())
				// Should trunc the file to size minus n returned here on error
				//err := binary.Write(s.data.db, binary.LittleEndian, se)
				if err != nil {
					log.WithError(err).Error("while writing event to buffer")
					if e.Status != nil {
						e.Status <- store.WriteStatus{
							Error: err,
						}
					}
					return
				}
				err = w.Flush()
				if err != nil {
					log.WithError(err).Error("while writing event to file")
					if e.Status != nil {
						e.Status <- store.WriteStatus{
							Error: err,
						}
					}
					return
				}
				s.data.len.Add(1)
				// s.data.position = se.Position
				if e.Status != nil {
					e.Status <- store.WriteStatus{
						Time:     se.Created,
						Position: se.Position,
					}
				}

				// Should not be needed as the file is opened with os.SYNC s.data.db.Sync() //Should add this outside a read while readable loop to reduce overhead, possibly
				s.data.newData.Broadcast()
			}()
		}
	}
}

func (s *Stream) Write() chan<- store.WriteEvent {
	return s.writeChan
}

func (s *Stream) Stream(
	from store.StreamPosition,
	ctx context.Context,
) (out <-chan store.ReadEvent, err error) {
	eventChan := make(chan store.ReadEvent, 2)
	out = eventChan
	go readStream(s, eventChan, uint64(from), ctx)
	return
}

func readStream(s *Stream, events chan<- store.ReadEvent, position uint64, ctx context.Context) {
	exit := false
	defer func() {
		if exit {
			close(events)
		}
	}()
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		log.WithError(fmt.Errorf("%v", r)).Error("recovering read stream", "stream", s.name)
		readStream(s, events, position, ctx)
	}()
	db, err := os.OpenFile(fmt.Sprintf("streams/%s", s.name), os.O_RDONLY, 0640)
	// db, err := os.OpenFile(es.data.db.Name(), os.O_RDONLY, 0640)
	if err != nil {
		log.WithError(err).Fatal("while opening stream file")
		return
	}
	r := db
	defer db.Close()
	select {
	case <-ctx.Done():
		exit = true
		return
	case <-s.ctx.Done():
		exit = true
		return
	default:
	}
	// position := uint64(from)
	if position == uint64(store.STREAM_END) {
		position = uint64(s.data.len.Load())
	}
	readTo := uint64(0)
	var se storeEvent
	for readTo < position {
		err = se.ReadBytes(r)
		// err = binary.Read(db, binary.LittleEndian, &se)
		if err != nil {
			if errors.Is(err, io.EOF) {
				time.Sleep(time.Millisecond * 250)
				continue
			}
			log.WithError(err).Fatal("while unmarshalling event from store catchup", "name", s.name)
		}
		readTo = se.Position
	}
	var start time.Time
	for !exit {
		log.Trace("starting new reader loop", "name", s.name)
		select {
		case <-ctx.Done():
			exit = true
			return
		case <-s.ctx.Done():
			exit = true
			return
		default:
			for err = se.ReadBytes(r); err == nil; err = se.ReadBytes(r) {
				if writeCount != nil {
					start = time.Now()
				}
				log.Trace("has more", "name", s.name)
				select {
				case <-ctx.Done():
					exit = true
					return
				case <-s.ctx.Done():
					exit = true
					return
				case events <- store.ReadEvent{
					Event:    se.Event,
					Position: se.Position,
					Created:  se.Created,
				}:
				}
				position = se.Position
				if readCount != nil {
					readCount.WithLabelValues(s.name).Inc()
					readTimeTotal.WithLabelValues(s.name).
						Add(float64(time.Since(start).Microseconds()))
				}
			}
			if !errors.Is(err, io.EOF) {
				time.Sleep(time.Second)
				log.WithError(err).Fatal("while reading event from store", "name", s.name)
			}
			log.Trace(
				"empty checking if there has come new data",
				"name",
				s.name,
				"p",
				position,
				"dl",
				s.data.len.Load(),
				// "dp",
				// s.data.position,
			)
			if position >= uint64(s.data.len.Load()) {
				log.Trace("waiting for new data", "name", s.name)
				s.data.newData.L.Lock()
				s.data.newData.Wait()
				s.data.newData.L.Unlock()
			} else {
				log.Warning("hit EOF with data left in file??", "pos", position, "stream_pos", s.data.len.Load())
				select {
				case <-ctx.Done():
					return
				case <-time.NewTimer(time.Second).C:
				}
			}
		}
	}
}

func (s *Stream) Name() string {
	return s.name
}

func (s *Stream) End() (pos uint64, err error) {
	// pos = s.data.position
	pos = s.data.len.Load()
	return
}
