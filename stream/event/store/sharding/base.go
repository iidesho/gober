package sharding

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event/store"
)

var log = sbragi.WithLocalScope(sbragi.LevelDebug)

// StreamFactory creates new stream instances
type StreamFactory interface {
	Init(name string, ctx context.Context) (stream.Stream, error)
}

// ShardedStream manages multiple shards with metadata tracking
type ShardedStream struct {
	meta       stream.Stream
	shards     map[string]stream.Stream
	shardsLock sync.RWMutex
	// nextPos    atomic.Uint64
	writeChan chan<- store.WriteEvent
	// router     ShardRouter
	// factory    StreamFactory
	factory func(name string, ctx context.Context) (es stream.Stream, err error)
	router  func(event store.Event) string
	name    string
}

// ShardRouter determines which shard an event goes to
type ShardRouter interface {
	Route(event store.Event) string
}

// MetadataEvent tracks global position and shard location
type MetadataEvent struct {
	ShardID string `json:"shard_id"`
}

func (e MetadataEvent) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0))
	if err != nil {
		return
	}
	err = bcts.WriteTinyString(w, e.ShardID)
	if err != nil {
		return
	}
	return nil
}

func (e *MetadataEvent) ReadBytes(r io.Reader) (err error) {
	var v uint8
	err = bcts.ReadUInt8(r, &v)
	if err != nil {
		return
	}
	if v != 0 {
		return fmt.Errorf("invalid stored event version, %s=%d, %s=%d", "expected", 0, "got", v)
	}
	err = bcts.ReadTinyString(r, &e.ShardID)
	if err != nil {
		return
	}
	return nil
}

func NewShardedStream(
	name string,
	// factory StreamFactory,
	// router ShardRouter,
	factory func(name string, ctx context.Context) (es stream.Stream, err error),
	router func(event store.Event) string,
	ctx context.Context,
) (*ShardedStream, error) {
	// Create metadata stream
	// meta, err := factory.Init(fmt.Sprintf("%s-meta", name), ctx)
	meta, err := factory(fmt.Sprintf("%s-meta", name), ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata stream: %w", err)
	}
	if meta == nil {
		return nil, fmt.Errorf("failed to create metadata stream: meta is nil")
	}

	writeChan := make(chan store.WriteEvent, 100)
	s := &ShardedStream{
		meta:      meta,
		shards:    make(map[string]stream.Stream),
		router:    router,
		factory:   factory,
		name:      name,
		writeChan: writeChan,
	}

	// Load existing shards from metadata
	if err := s.loadShardsFromMeta(ctx); err != nil {
		return nil, fmt.Errorf("failed to load shards: %w", err)
	}

	// Initialize nextPos from meta stream end
	// endPos, _ := meta.End()
	// s.nextPos.Store(uint64(endPos))

	go func(writeChan <-chan store.WriteEvent, ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case we := <-writeChan:
				pos, err := s.write(we.Event, ctx)
				if we.Status != nil {
					if err != nil {
						select {
						case <-ctx.Done():
							return
						case we.Status <- store.WriteStatus{
							Error: err,
						}:
						}
					} else {
						select {
						case <-ctx.Done():
							return
						case we.Status <- store.WriteStatus{
							Position: pos,
						}:
						}
					}
					close(we.Status)
				}
			}
		}
	}(writeChan, ctx)

	return s, nil
}

func (s *ShardedStream) loadShardsFromMeta(ctx context.Context) error {
	metaStream, err := s.meta.Stream(0, ctx)
	if err != nil {
		return err
	}

	seenShards := make(map[string]bool)

	for {
		select {
		case <-time.After(time.Millisecond * 150):
			return nil
		case event := <-metaStream:
			meta, err := bcts.Read[MetadataEvent](event.Data)
			if log.WithError(err).Error("reading metadata event") {
				continue
			}

			if !seenShards[meta.ShardID] {
				seenShards[meta.ShardID] = true
				// shard, err := s.factory.Init(fmt.Sprintf("%s-shard-%s", s.name, meta.ShardID), ctx)
				shard, err := s.factory(fmt.Sprintf("%s-shard-%s", s.name, meta.ShardID), ctx)
				if err != nil {
					return fmt.Errorf("failed to load shard %s: %w", meta.ShardID, err)
				}
				s.shards[meta.ShardID] = shard
			}
		}
	}

	// return nil
}

func (s *ShardedStream) getOrCreateShard(
	shardID string,
	ctx context.Context,
) (stream.Stream, error) {
	s.shardsLock.RLock()
	shard, exists := s.shards[shardID]
	s.shardsLock.RUnlock()

	if exists {
		return shard, nil
	}

	s.shardsLock.Lock()
	defer s.shardsLock.Unlock()

	// Double-check after acquiring write lock
	if shard, exists := s.shards[shardID]; exists {
		return shard, nil
	}

	// Create new shard
	// shard, err := s.factory.Init(fmt.Sprintf("%s-shard-%s", s.name, shardID), ctx)
	shard, err := s.factory(fmt.Sprintf("%s-shard-%s", s.name, shardID), ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create shard %s: %w", shardID, err)
	}

	s.shards[shardID] = shard
	return shard, nil
}

func (s *ShardedStream) write(
	event store.Event,
	ctx context.Context,
) (store.StreamPosition, error) {
	// shardID := s.router.Route(event)
	shardID := s.router(event)

	shard, err := s.getOrCreateShard(shardID, ctx)
	if err != nil {
		return 0, err
	}

	data, err := bcts.Write(MetadataEvent{
		ShardID: shardID,
	})
	if err != nil {
		return 0, err
	}
	mws := make(chan store.WriteStatus, 1)
	mwe := store.WriteEvent{
		Status: mws,
		Event: store.Event{
			Type: event.Type,
			ID:   event.ID,
			Data: data,
		},
	}
	s.meta.Write() <- mwe
	metaStat := <-mws
	if metaStat.Error != nil {
		return 0, metaStat.Error
	}

	event.GlobalPos = metaStat.Position
	ws := make(chan store.WriteStatus, 1)
	we := store.WriteEvent{
		Status: ws,
		Event:  event,
	}

	// Write to shard first
	shard.Write() <- we

	// Get shard position after write
	stat := <-ws
	if stat.Error != nil {
		return 0, stat.Error
	}
	return metaStat.Position, nil
}

// ReadShard returns raw shard stream without position rewriting
func (s *ShardedStream) StreamShard(
	shardID string,
	from store.StreamPosition,
	ctx context.Context,
) (<-chan store.ReadEvent, error) {
	s.shardsLock.RLock()
	shard, exists := s.shards[shardID]
	s.shardsLock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("shard %s not found", shardID)
	}

	return shard.Stream(from, ctx)
}

// GetShardIDs returns all known shard IDs
func (s *ShardedStream) GetShardIDs() []string {
	s.shardsLock.RLock()
	defer s.shardsLock.RUnlock()

	ids := make([]string, 0, len(s.shards))
	for id := range s.shards {
		ids = append(ids, id)
	}
	return ids
}

// ZipperReader reads from all shards with global position ordering
// type ZipperReader struct {
// 	shards map[string]stream.Stream
// 	meta   stream.Stream
// 	// factory StreamFactory
// 	factory func(name string, ctx context.Context) (es stream.Stream, err error)
// 	name    string
// }

// func (s *ShardedStream) NewZipperReader() *ZipperReader {
// 	s.shardsLock.RLock()
// 	shardsCopy := make(map[string]stream.Stream, len(s.shards))
// 	for k, v := range s.shards {
// 		shardsCopy[k] = v
// 	}
// 	s.shardsLock.RUnlock()
//
// 	return &ZipperReader{
// 		meta:    s.meta,
// 		shards:  shardsCopy,
// 		factory: s.factory,
// 		name:    s.name,
// 	}
// }

func (s *ShardedStream) Stream(
	from store.StreamPosition,
	ctx context.Context,
) (<-chan store.ReadEvent, error) {
	out := make(chan store.ReadEvent)
	if s == nil {
		log.Fatal("This sucks")
	}

	go func() {
		defer close(out)

		// Read metadata to build position map
		// log.Info("strange", "from", from, "ctx", ctx, "s", s)
		metaStream, err := s.meta.Stream(from, ctx)
		if err != nil {
			return
		}

		// Map of shard -> reader channel
		shardReaders := make(map[string]<-chan store.ReadEvent)
		// Map of shard -> last read position
		shardPositions := make(map[string]store.StreamPosition)
		// Priority queue of pending events
		// pending := &eventHeap{}
		// heap.Init(pending)

		for {
			select {
			case <-ctx.Done():
				return

			case metaEvent, ok := <-metaStream:
				if !ok {
					// Drain remaining events
					// for pending.Len() > 0 {
					// 	item := heap.Pop(pending).(*eventHeapItem)
					// 	select {
					// 	case out <- item.event:
					// 	case <-ctx.Done():
					// 		return
					// 	}
					// }
					return
				}

				// Parse metadata
				meta, err := bcts.Read[MetadataEvent](metaEvent.Data)
				if log.WithError(err).Error("reading metadata event") {
					return
				}

				// Initialize shard reader if needed
				var reader <-chan store.ReadEvent
				var exists bool
				if reader, exists = shardReaders[meta.ShardID]; !exists {
					// Try to get existing shard or create new one
					shard, exists := s.shards[meta.ShardID]
					if !exists {
						// Dynamically load shard if not in initial map
						// shard, err = z.factory.Init(
						shard, err = s.factory(
							fmt.Sprintf("%s-shard-%s", s.name, meta.ShardID),
							ctx,
						)
						if err != nil {
							continue
						}
						s.shards[meta.ShardID] = shard
					}

					reader, err = shard.Stream(0, ctx)
					if err != nil {
						continue
					}
					shardReaders[meta.ShardID] = reader
					shardPositions[meta.ShardID] = 0
				}

				select {
				case shardEvent, ok := <-reader:
					if !ok {
						break
					}
					shardEvent.Position = shardEvent.GlobalPos
					select {
					case out <- shardEvent:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
				// // Read from shard until we reach the position
				// reader := shardReaders[meta.ShardID]
				// for shardPositions[meta.ShardID] < meta.ShardPos {
				// 	select {
				// 	case shardEvent, ok := <-reader:
				// 		if !ok {
				// 			break
				// 		}
				// 		shardPositions[meta.ShardID] = shardEvent.Position
				//
				// 		if shardEvent.Position == meta.ShardPos {
				// 			// Rewrite position to global
				// 			shardEvent.Position = meta.GlobalPos
				//
				// 			// Add to heap
				// 			heap.Push(pending, &eventHeapItem{
				// 				event: shardEvent,
				// 				index: int(meta.GlobalPos),
				// 			})
				// 		}
				// 	case <-ctx.Done():
				// 		return
				// 	}
				// }
				//
				// // Output events in order
				// for pending.Len() > 0 {
				// 	item := (*pending)[0]
				// 	if item.index <= int(meta.GlobalPos) {
				// 		heap.Pop(pending)
				// 		select {
				// 		case out <- item.event:
				// 		case <-ctx.Done():
				// 			return
				// 		}
				// 	} else {
				// 		break
				// 	}
				// }
			}
		}
	}()

	return out, nil
}

func (s *ShardedStream) Write() chan<- store.WriteEvent {
	return s.writeChan
}

func (s *ShardedStream) Name() string {
	return s.meta.Name()
}

func (s *ShardedStream) End() (pos store.StreamPosition, err error) {
	return s.meta.End()
}

// StaticRouter routes based on hash of a key
type StaticRouter struct{}

func (r StaticRouter) Route(event store.Event) string {
	return event.Shard
}
