package sharding

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/stream/event/store/ondisk"
)

// Test helpers
func createFactory() func(string, context.Context) (stream.Stream, error) {
	return func(name string, ctx context.Context) (stream.Stream, error) {
		return ondisk.Init(name, ctx)
	}
}

func createRouter() func(store.Event) string {
	return StaticRouter{}.Route
}

func TestPreClean(t *testing.T) {
	os.RemoveAll("./streams")
}

// Tests
func TestShardedStreamBasicWrite(t *testing.T) {
	ctx := context.Background()

	sharded, err := NewShardedStream(
		"test1",
		createFactory(),
		createRouter(),
		ctx,
	)
	if err != nil {
		t.Fatalf("Failed to create sharded stream: %v", err)
	}

	// Write events
	events := []store.Event{
		{ID: uuid.Must(uuid.NewV7()), Shard: "s1", Data: []byte("data1")},
		{ID: uuid.Must(uuid.NewV7()), Shard: "s2", Data: []byte("data2")},
		{ID: uuid.Must(uuid.NewV7()), Shard: "s1", Data: []byte("data3")},
	}

	for _, e := range events {
		sharded.Write() <- store.WriteEvent{Event: e}
		// if err := sharded.Write(e); err != nil {
		// 	t.Errorf("Failed to write event %s: %v", e.ID, err)
		// }
	}

	// Allow writes to process
	time.Sleep(100 * time.Millisecond)

	// Check metadata stream has correct number of events
	metaPos, _ := sharded.meta.End()
	if metaPos != 3 {
		t.Errorf("Expected 3 metadata events, got %d", metaPos)
	}
}

func TestZipperReaderOrdering(t *testing.T) {
	ctx := context.Background()

	sharded, err := NewShardedStream("test2",
		createFactory(),
		createRouter(),
		ctx)
	if err != nil {
		t.Fatalf("Failed to create sharded stream: %v", err)
	}

	// Write events to different shards
	events := []store.Event{
		{ID: uuid.Must(uuid.NewV7()), Shard: "s1", Data: []byte("data1")},
		{ID: uuid.Must(uuid.NewV7()), Shard: "s2", Data: []byte("data2")},
		{ID: uuid.Must(uuid.NewV7()), Shard: "s1", Data: []byte("data3")},
	}

	for _, e := range events {
		sharded.Write() <- store.WriteEvent{Event: e}
		// if err := sharded.Write(e); err != nil {
		// 	t.Errorf("Failed to write event %s: %v", e.ID, err)
		// }
	}

	time.Sleep(100 * time.Millisecond)

	// Read all events through zipper
	// zipper := sharded.NewZipperReader()
	readCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	stream, err := sharded.Stream(0, readCtx)
	if err != nil {
		t.Fatalf("Failed to create zipper stream: %v", err)
	}

	var readEvents []store.ReadEvent
	for e := range stream {
		readEvents = append(readEvents, e)
	}

	// Verify we got all events
	if len(readEvents) != len(events) {
		t.Errorf(
			"Expected %d events, got %d, want %v has %v",
			len(events),
			len(readEvents),
			events,
			readEvents,
		)
	}

	// Verify positions are sequential
	for i, e := range readEvents {
		expectedPos := store.StreamPosition(i + 1)
		if e.Position != expectedPos {
			t.Errorf("Event %d has position %d, expected %d", i, e.Position, expectedPos)
		}
	}
}

func TestReadShard(t *testing.T) {
	ctx := context.Background()

	router := createRouter()
	sharded, err := NewShardedStream("test3", createFactory(), router, ctx)
	if err != nil {
		t.Fatalf("Failed to create sharded stream: %v", err)
	}

	// Write events to ensure specific shard exists
	event := store.Event{
		ID:    uuid.Must(uuid.NewV7()),
		Shard: "s10",
		Data:  []byte("test data"),
	}
	shardID := router(event)

	sharded.Write() <- store.WriteEvent{Event: event}
	// if err := sharded.Write(event); err != nil {
	// 	t.Fatalf("Failed to write event: %v", err)
	// }

	time.Sleep(100 * time.Millisecond)

	// Read from specific shard with timeout context
	readCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	stream, err := sharded.StreamShard(shardID, 0, readCtx)
	if err != nil {
		t.Fatalf("Failed to read shard: %v", err)
	}

	var count int
	for {
		select {
		case <-readCtx.Done():
			// Context cancelled, we're done reading
			goto done
		case _, ok := <-stream:
			if !ok {
				goto done
			}
			count++
		}
	}
done:

	if count != 1 {
		t.Errorf("Expected 1 event from shard, got %d", count)
	}
}

func TestDynamicShardCreation(t *testing.T) {
	ctx := context.Background()

	sharded, err := NewShardedStream("test4", createFactory(), createRouter(), ctx)
	if err != nil {
		t.Fatalf("Failed to create sharded stream: %v", err)
	}

	initialShards := len(sharded.GetShardIDs())

	// Write events with different IDs to potentially create new shards
	for i := 0; i < 20; i++ {
		event := store.Event{
			ID:    uuid.Must(uuid.NewV7()),
			Shard: strconv.Itoa(i % 3),
			Data:  []byte(fmt.Sprintf("data%d", i)),
		}
		sharded.Write() <- store.WriteEvent{Event: event}
		// if err := sharded.Write(event); err != nil {
		// 	t.Errorf("Failed to write event %d: %v", i, err)
		// }
	}

	time.Sleep(100 * time.Millisecond)

	finalShards := len(sharded.GetShardIDs())
	if finalShards <= initialShards {
		t.Errorf(
			"Expected more shards after writes, initial: %d, final: %d",
			initialShards,
			finalShards,
		)
	}
}

// Benchmarks
func BenchmarkShardedWrite(b *testing.B) {
	ctx := context.Background()

	sharded, err := NewShardedStream("bench1", createFactory(), createRouter(), ctx)
	if err != nil {
		b.Fatalf("Failed to create sharded stream: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			event := store.Event{
				ID:   uuid.Must(uuid.NewV7()),
				Data: []byte("benchmark data"),
			}
			sharded.Write() <- store.WriteEvent{Event: event}
			// _ = sharded.Write(event)
		}
	})
}

func BenchmarkZipperRead(b *testing.B) {
	ctx := context.Background()

	sharded, err := NewShardedStream("bench2", createFactory(), createRouter(), ctx)
	if err != nil {
		b.Fatalf("Failed to create sharded stream: %v", err)
	}

	// Pre-populate with events
	for i := 0; i < 1000; i++ {
		event := store.Event{
			ID:    uuid.Must(uuid.NewV7()),
			Shard: strconv.Itoa(i % 10),
			Data:  []byte(fmt.Sprintf("data%d", i)),
		}
		sharded.Write() <- store.WriteEvent{Event: event}
		// _ = sharded.Write(event)
	}

	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// zipper := sharded.NewZipperReader()
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		stream, _ := sharded.Stream(0, readCtx)
		count := 0
		for range stream {
			count++
			if count >= 100 {
				cancel()
				break
			}
		}
		cancel()
	}
}

func BenchmarkDirectShardRead(b *testing.B) {
	ctx := context.Background()

	router := createRouter()
	sharded, err := NewShardedStream("bench3", createFactory(), router, ctx)
	if err != nil {
		b.Fatalf("Failed to create sharded stream: %v", err)
	}

	// Pre-populate with same routing key
	// testID := uuid.Must(uuid.NewV7())
	// shardID := router(store.Event{ID: testID})

	for i := 0; i < 1000; i++ {
		// Use sequential IDs that hash to same shard
		event := store.Event{
			ID:    uuid.Must(uuid.NewV7()),
			Shard: strconv.Itoa(i % 10),
			Data:  []byte(fmt.Sprintf("data%d", i)),
		}
		sharded.Write() <- store.WriteEvent{Event: event}
		// _ = sharded.Write(event)
	}

	time.Sleep(200 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)

		stream, _ := sharded.StreamShard(strconv.Itoa(i%10), 0, readCtx)
		count := 0
		for range stream {
			count++
			if count >= 100 {
				cancel()
				break
			}
		}
		cancel()
	}
}

// func BenchmarkConcurrentMixedOperations(b *testing.B) {
// 	ctx := context.Background()
//
// 	sharded, err := NewShardedStream("bench", createFactory(), createRouter(), ctx)
// 	if err != nil {
// 		b.Fatalf("Failed to create sharded stream: %v", err)
// 	}
//
// 	b.ResetTimer()
// 	b.RunParallel(func(pb *testing.PB) {
// 		i := 0
// 		for pb.Next() {
// 			if i%3 == 0 {
// 				// Write
// 				event := store.Event{
// 					ID:       uuid.Must(uuid.NewV7()),
// 					ShardKey: strconv.Itoa(i % 10),
// 					Data:     []byte(fmt.Sprintf("data%d", i)),
// 				}
// 				_ = sharded.Write(event)
// 			} else if i%3 == 1 {
// 				// Read specific shard
// 				readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
// 				stream, _ := sharded.ReadShard(strconv.Itoa(i%10), 0, readCtx)
// 				for range stream {
// 					break
// 				}
// 				cancel()
// 			} else {
// 				// Zipper read (limited)
// 				zipper := sharded.NewZipperReader()
// 				readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
// 				stream, _ := zipper.ReadAll(0, readCtx)
// 				count := 0
// 				for range stream {
// 					count++
// 					if count >= 10 {
// 						break
// 					}
// 				}
// 				cancel()
// 			}
// 			i++
// 		}
// 	})
// }
