package mariadb

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/metrics"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/stream/event/store/ondisk"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	log            = sbragi.WithLocalScope(sbragi.LevelInfo)
	writeCount     *prometheus.CounterVec
	writeTimeTotal *prometheus.CounterVec
	readCount      *prometheus.CounterVec
	readTimeTotal  *prometheus.CounterVec
	tableNameRegex = regexp.MustCompile(`[^a-zA-Z0-9_]`)
)

type Stream struct {
	ctx       context.Context
	db        *sql.DB
	writeChan chan store.WriteEvent
	name      string
	len       *atomic.Uint64
	newData   *sync.Cond
}

func Init(name string, ctx context.Context) (*Stream, error) {
	dsn := "admin:your_password@tcp(127.0.0.1:3306)/?parseTime=true&multiStatements=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Create database if not exists
	_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS eventstore")
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// Try to set max_allowed_packet globally (requires SUPER privilege)
	// _, err = db.ExecContext(ctx, "SET GLOBAL max_allowed_packet=67108864")
	_, err = db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL max_allowed_packet=%d", ondisk.GB*16))
	if err != nil {
		log.WithError(err).Info("failed to set global max_allowed_packet, using default")
	}

	// Check current max_allowed_packet
	var maxPacket int64
	err = db.QueryRowContext(ctx, "SELECT @@max_allowed_packet").Scan(&maxPacket)
	if err == nil {
		log.Info("current max_allowed_packet", "bytes", maxPacket, "MB", maxPacket/1024/1024)
	}

	dsn = fmt.Sprintf(
		"admin:your_password@tcp(127.0.0.1:3306)/eventstore?parseTime=true&multiStatements=true&maxAllowedPacket=67108864",
	)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	if err := createTable(ctx, db, name); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	position, err := getLastPosition(ctx, db, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get last position: %w", err)
	}

	s := &Stream{
		ctx:       ctx,
		db:        db,
		writeChan: make(chan store.WriteEvent, 100),
		name:      name,
		len:       &atomic.Uint64{},
		newData:   sync.NewCond(&sync.Mutex{}),
	}

	s.len.Store(uint64(position))

	if err := initMetrics(); err != nil {
		return nil, err
	}

	go s.writeStream()

	return s, nil
}

func createTable(ctx context.Context, db *sql.DB, name string) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			position BIGINT UNSIGNED PRIMARY KEY,
			event_id CHAR(36) NOT NULL,
			event_type VARCHAR(255) NOT NULL,
			event_data LONGBLOB,
			event_metadata LONGBLOB,
			created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			INDEX idx_created_at (created_at)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
	`, sanitizeTableName(name))

	_, err := db.ExecContext(ctx, query)
	return err
}

func getLastPosition(ctx context.Context, db *sql.DB, name string) (store.StreamPosition, error) {
	query := fmt.Sprintf("SELECT COALESCE(MAX(position), 0) FROM %s", sanitizeTableName(name))

	var position uint64
	err := db.QueryRowContext(ctx, query).Scan(&position)
	if err != nil {
		return store.STREAM_START, err
	}

	return store.StreamPosition(position), nil
}

func sanitizeTableName(name string) string {
	// Replace invalid characters with underscores
	cleaned := tableNameRegex.ReplaceAllString(name, "_")

	// Add prefix
	tableName := fmt.Sprintf("stream_%s", cleaned)

	// If the name is too long (>64 chars), hash it
	if len(tableName) > 64 {
		// Use first 20 chars + hash of full name
		hash := md5.Sum([]byte(name))
		hashStr := hex.EncodeToString(hash[:])[:16]
		if len(cleaned) > 20 {
			cleaned = cleaned[:20]
		}
		tableName = fmt.Sprintf("stream_%s_%s", cleaned, hashStr)
	}

	// Ensure it doesn't start with a number
	if len(tableName) > 0 && tableName[0] >= '0' && tableName[0] <= '9' {
		tableName = "s_" + tableName
	}

	return fmt.Sprintf("`%s`", strings.ToLower(tableName))
}

func (s *Stream) writeStream() {
	defer func() {
		if r := recover(); r != nil {
			log.WithError(fmt.Errorf("%v", r)).Error("recovering write stream", "stream", s.name)
			go s.writeStream()
		}
	}()

	batch := make([]store.WriteEvent, 0, 1000)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case e := <-s.writeChan:
			batch = append(batch, e)

			// Collect more events if available
		collectMore:
			for len(batch) < 1000 {
				select {
				case e := <-s.writeChan:
					batch = append(batch, e)
				case <-ticker.C:
					break collectMore
				default:
					break collectMore
				}
			}

			if len(batch) > 0 {
				s.writeBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				s.writeBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func (s *Stream) writeBatch(events []store.WriteEvent) {
	if len(events) == 0 {
		return
	}

	tx, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		log.WithError(err).Error("failed to begin transaction")
		for _, e := range events {
			if e.Status != nil {
				e.Status <- store.WriteStatus{Error: err}
				close(e.Status)
			}
		}
		return
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
		INSERT INTO %s (position, event_id, event_type, event_data, event_metadata, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, sanitizeTableName(s.name))

	stmt, err := tx.PrepareContext(s.ctx, query)
	if err != nil {
		log.WithError(err).Error("failed to prepare statement")
		for _, e := range events {
			if e.Status != nil {
				e.Status <- store.WriteStatus{Error: err}
				close(e.Status)
			}
		}
		return
	}
	defer stmt.Close()

	basePos := s.len.Load()
	now := time.Now()

	for i, e := range events {
		if writeCount != nil {
			start := time.Now()
			defer func() {
				writeCount.WithLabelValues(s.name).Inc()
				writeTimeTotal.WithLabelValues(s.name).
					Add(float64(time.Since(start).Microseconds()))
			}()
		}

		pos := basePos + uint64(i) + 1

		_, err := stmt.ExecContext(s.ctx,
			pos,
			e.Event.ID.String(),
			e.Event.Type,
			e.Event.Data,
			e.Event.Metadata,
			now,
		)
		if err != nil {
			log.WithError(err).Error("failed to insert event")
			if e.Status != nil {
				e.Status <- store.WriteStatus{Error: err}
				close(e.Status)
			}
			continue
		}

		if e.Status != nil {
			e.Status <- store.WriteStatus{
				Time:     now,
				Position: store.StreamPosition(pos),
			}
			close(e.Status)
		}
	}

	if err := tx.Commit(); err != nil {
		log.WithError(err).Error("failed to commit transaction")
		return
	}

	s.len.Add(uint64(len(events)))
	s.newData.Broadcast()
}

func (s *Stream) Write() chan<- store.WriteEvent {
	return s.writeChan
}

func (s *Stream) Stream(
	from store.StreamPosition,
	ctx context.Context,
) (<-chan store.ReadEvent, error) {
	eventChan := make(chan store.ReadEvent, 1000)
	go s.readStream(eventChan, from, ctx)
	return eventChan, nil
}

func (s *Stream) readStream(
	events chan<- store.ReadEvent,
	position store.StreamPosition,
	ctx context.Context,
) {
	exit := false
	defer func() {
		if exit {
			close(events)
		}
	}()

	defer func() {
		if exit {
			return
		}
		if r := recover(); r != nil {
			log.WithError(fmt.Errorf("%v", r)).Error("recovering read stream", "stream", s.name)
			go s.readStream(events, position, ctx)
		}
	}()

	if position == store.STREAM_END {
		position = store.StreamPosition(s.len.Load())
	}

	lastPosition := position
	pollInterval := 10 * time.Millisecond
	maxPollInterval := 100 * time.Millisecond

	for !exit {
		select {
		case <-ctx.Done():
			exit = true
			return
		case <-s.ctx.Done():
			exit = true
			return
		default:
			query := fmt.Sprintf(`
				SELECT position, event_id, event_type, event_data, event_metadata, created_at
				FROM %s
				WHERE position > ?
				ORDER BY position ASC
				LIMIT 1000
			`, sanitizeTableName(s.name))

			rows, err := s.db.QueryContext(ctx, query, lastPosition)
			if err != nil {
				log.WithError(err).Error("failed to query events")
				time.Sleep(pollInterval)
				continue
			}

			hasEvents := false
			for rows.Next() {
				if readCount != nil {
					start := time.Now()
					defer func() {
						readCount.WithLabelValues(s.name).Inc()
						readTimeTotal.WithLabelValues(s.name).
							Add(float64(time.Since(start).Microseconds()))
					}()
				}

				var (
					pos       uint64
					eventID   string
					eventType string
					eventData []byte
					eventMeta []byte
					createdAt time.Time
				)

				if err := rows.Scan(&pos, &eventID, &eventType, &eventData, &eventMeta, &createdAt); err != nil {
					log.WithError(err).Error("failed to scan row")
					continue
				}

				id, _ := uuid.FromString(eventID)

				select {
				case <-ctx.Done():
					rows.Close()
					exit = true
					return
				case <-s.ctx.Done():
					rows.Close()
					exit = true
					return
				case events <- store.ReadEvent{
					Event: store.Event{
						ID:       id,
						Type:     eventType,
						Data:     eventData,
						Metadata: eventMeta,
					},
					Position: store.StreamPosition(pos),
					Created:  createdAt,
				}:
					lastPosition = store.StreamPosition(pos)
					hasEvents = true
				}
			}
			rows.Close()

			if !hasEvents {
				if lastPosition >= store.StreamPosition(s.len.Load()) {
					s.newData.L.Lock()
					if lastPosition >= store.StreamPosition(s.len.Load()) {
						s.newData.Wait()
					}
					s.newData.L.Unlock()
				} else {
					time.Sleep(pollInterval)
					if pollInterval < maxPollInterval {
						pollInterval = pollInterval * 2
					}
				}
			} else {
				pollInterval = 10 * time.Millisecond
			}
		}
	}
}

func (s *Stream) Name() string {
	return s.name
}

func (s *Stream) End() (store.StreamPosition, error) {
	return store.StreamPosition(s.len.Load()), nil
}

func initMetrics() error {
	if metrics.Registry == nil || writeCount != nil {
		return nil
	}

	writeCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mariadb_event_write_count",
		Help: "mariadb event write count",
	}, []string{"stream"})
	if err := metrics.Registry.Register(writeCount); err != nil {
		return err
	}

	writeTimeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mariadb_event_write_time_total",
		Help: "mariadb event write time total",
	}, []string{"stream"})
	if err := metrics.Registry.Register(writeTimeTotal); err != nil {
		return err
	}

	readCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mariadb_event_read_count",
		Help: "mariadb event read count",
	}, []string{"stream"})
	if err := metrics.Registry.Register(readCount); err != nil {
		return err
	}

	readTimeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "mariadb_event_read_time_total",
		Help: "mariadb event read time total",
	}, []string{"stream"})
	if err := metrics.Registry.Register(readTimeTotal); err != nil {
		return err
	}

	return nil
}
