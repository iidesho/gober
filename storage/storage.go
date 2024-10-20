package storage

import (
	"encoding/binary"
	"iter"
	"os"

	"github.com/iidesho/bragi/sbragi"
	jsoniter "github.com/json-iterator/go"
	"github.com/nutsdb/nutsdb"
)

var json = jsoniter.ConfigFastest

type PosStorage[T any] interface {
	Storage[T]

	SetUInt64(v uint64) error
	GetUInt64() (v uint64, err error)
}

type Storage[T any] interface {
	Set(k string, v T, opts ...OptFunc) error
	Get(k string) (v T, err error)
	Delete(k string, opts ...OptFunc) error
	Range() iter.Seq2[string, T]
}

type storage[T any] struct {
	db     *nutsdb.DB
	posKey []byte
}

/*
type badgerLogger struct {
	sbragi.Logger
}

func (bl badgerLogger) Errorf(msg string, args ...interface{}) {
	bl.Error(fmt.Sprintf(msg, args...))
}
func (bl badgerLogger) Warningf(msg string, args ...interface{}) {
	bl.Warning(fmt.Sprintf(msg, args...))
}
func (bl badgerLogger) Infof(msg string, args ...interface{}) {
	bl.Info(fmt.Sprintf(msg, args...))
}
func (bl badgerLogger) Debugf(msg string, args ...interface{}) {
	bl.Debug(fmt.Sprintf(msg, args...))
}
*/

func New[T any](dir string) Storage[T] {
	sbragi.WithError(os.MkdirAll(dir, 0750)).Fatal("creating dir")
	//db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(badgerLogger{sbragi.DefaultLogger()}))
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(dir),
	)
	sbragi.WithError(err).Fatal("opening kv store", "dir", dir)
	err = db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewKVBucket("bucket?")
	})
	sbragi.WithoutEscalation().WithError(err).Debug("creating kv bucket", "dir", dir)
	//defer func() { db.Close() }()
	return storage[T]{
		db: db,
	}
}

func NewWPos[T any](dir, posKey string) PosStorage[T] {
	sbragi.WithError(os.MkdirAll(dir, 0750)).Fatal("creating dir")
	//db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(badgerLogger{sbragi.DefaultLogger()}))
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(dir),
	)
	sbragi.WithError(err).Fatal("opening kv store", "dir", dir)
	err = db.Update(func(tx *nutsdb.Tx) error {
		return tx.NewKVBucket("bucket?")
	})
	sbragi.WithError(err).Debug("creating kv bucket", "dir", dir)
	//defer func() { db.Close() }()
	return storage[T]{
		db:     db,
		posKey: []byte(posKey),
	}
}

type Opt struct {
	pos *uint64
	ttl int
}

type OptFunc func(*Opt)

func OptTTL(ttl int) OptFunc {
	return func(o *Opt) {
		o.ttl = ttl
	}
}

func OptPos(pos uint64) OptFunc {
	return func(o *Opt) {
		o.pos = &pos
	}
}

func (s storage[T]) Set(k string, v T, optFuncs ...OptFunc) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	opt := Opt{}
	for _, of := range optFuncs {
		of(&opt)
	}
	sbragi.Info("storing", "key", k, "val", string(b))
	return s.db.Update(func(tx *nutsdb.Tx) error {
		err := tx.Put("bucket?", []byte(k), b, uint32(opt.ttl))
		if err != nil {
			return err
		}
		if s.posKey != nil && opt.pos != nil {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, *opt.pos)
			return tx.Put("bucket?", s.posKey, b, 0)
		}
		return nil
	})
}

func (s storage[T]) Get(k string) (v T, err error) {
	sbragi.Info("getting", "key", k)
	var data []byte
	err = s.db.View(func(tx *nutsdb.Tx) error {
		data, err = tx.Get("bucket?", []byte(k))
		return err
	})
	if err != nil {
		return
	}
	err = json.Unmarshal(data, &v)
	return
}

func (s storage[T]) Delete(k string, optFuncs ...OptFunc) error {
	sbragi.Info("deleteing", "key", k)
	opt := Opt{}
	for _, of := range optFuncs {
		of(&opt)
	}
	return s.db.Update(func(tx *nutsdb.Tx) error {
		err := tx.Delete("bucket?", []byte(k))
		if err != nil {
			return err
		}
		if s.posKey != nil && opt.pos != nil {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, *opt.pos)
			return tx.Put("bucket?", s.posKey, b, 0)
		}
		return nil
	})
}

func (s storage[T]) SetUInt64(v uint64) error {
	return s.db.Update(func(tx *nutsdb.Tx) error {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, v)
		return tx.Put("bucket?", s.posKey, b, 0)
	})
}

func (s storage[T]) GetUInt64() (v uint64, err error) {
	var data []byte
	err = s.db.View(func(tx *nutsdb.Tx) error {
		data, err = tx.Get("bucket?", s.posKey)
		return err
	})
	if err != nil {
		return
	}
	v = binary.LittleEndian.Uint64(data)
	return
}

func (s storage[T]) Range() iter.Seq2[string, T] {
	var keys [][]byte
	var values [][]byte
	err := s.db.View(func(tx *nutsdb.Tx) error {
		var err error
		keys, values, err = tx.GetAll("bucket?")
		return err
	})
	sbragi.WithError(err).Error("getting values for range")
	return func(yield func(string, T) bool) {
		var v T
		for i := range keys {
			err = json.Unmarshal(values[i], &v)
			if sbragi.WithError(err).
				Error("unmarshaling value", "key", string(keys[i]), "raw_value", string(values[i])) {
				continue
			}
			if !yield(string(keys[i]), v) {
				return
			}
		}
	}
}
