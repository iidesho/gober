package persistenteventmap

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/dgraph-io/badger/options"

	"github.com/dgraph-io/badger"

	log "github.com/cantara/bragi"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
)

type EventMap[DT any] interface {
	Get(key string) (data DT, err error)
	Exists(key string) (exists bool)
	Len() (l int)
	Keys() (keys []string)
	Range(f func(key string, data DT) error)
	Delete(data DT) (err error)
	Set(data DT) (err error)
}

type transactionCheck struct {
}

type mapData[DT any] struct {
	data            *badger.DB
	transactionChan chan transactionCheck
	dataTypeName    string
	dataTypeVersion string
	provider        stream.CryptoKeyProvider
	es              stream.Stream
	ctx             context.Context
	getKey          func(dt DT) string
	esh             stream.SetHelper
}

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, getKey func(dt DT) string, ctx context.Context) (ed EventMap[DT], err error) {
	db, err := badger.Open(badger.DefaultOptions("./eventmap/" + dataTypeName).
		WithMaxTableSize(1024 * 1024 * 8).
		WithValueLogFileSize(1024 * 1024 * 8).
		WithValueLogLoadingMode(options.FileIO))
	if err != nil {
		return
	}
	m := mapData[DT]{
		data:            db,
		transactionChan: make(chan transactionCheck),
		dataTypeName:    dataTypeName,
		dataTypeVersion: dataTypeVersion,
		provider:        p,
		es:              s,
		getKey:          getKey,
	}
	from := store.STREAM_START
	positionKey := []byte(fmt.Sprintf("%s_%s_position", s.Name(), dataTypeName))
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(positionKey)
		if err != nil {
			return err
		}
		var pos uint64
		err = item.Value(func(val []byte) error {
			pos = uint64(binary.LittleEndian.Uint64(val))
			return nil
		})
		if err != nil {
			return err
		}
		from = store.StreamPosition(pos)
		return nil
	})
	eventTypes := event.AllTypes()
	eventChan, err := stream.NewStream[DT](s, eventTypes, from, stream.ReadDataType(dataTypeName), p, ctx)
	if err != nil {
		return
	}
	m.esh, err = stream.InitSetHelper(func(e event.Event[DT]) {
		data, err := json.Marshal(e.Data)
		if err != nil {
			return
		}
		err = db.Update(func(txn *badger.Txn) error {
			err = txn.Set([]byte(getKey(e.Data)), data)
			if err != nil {
				return err
			}

			pos := make([]byte, 8)
			binary.LittleEndian.PutUint64(pos, e.Position)
			return txn.Set(positionKey, pos)
		})
		if err != nil {
			log.AddError(err).Warning("Update error")
		}
	}, func(e event.Event[DT]) {
		err := db.Update(func(txn *badger.Txn) error {
			err = txn.Delete([]byte(getKey(e.Data)))
			if err != nil {
				return err
			}

			pos := make([]byte, 8)
			binary.LittleEndian.PutUint64(pos, e.Position)
			return txn.Set(positionKey, pos)
		})
		if err != nil {
			log.AddError(err).Warning("Delete error")
		}
	}, m.es, p, eventChan, ctx)
	if err != nil {
		return
	}

	ed = &m
	return
}

var ERROR_KEY_NOT_FOUND = fmt.Errorf("provided key does not exist")

func (m mapData[DT]) Get(key string) (data DT, err error) {
	var ed []byte
	err = m.data.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		ed, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}

	err = json.Unmarshal(ed, &data)
	if err != nil {
		return
	}
	return
}

func (m mapData[DT]) Len() (l int) {
	return len(m.Keys())
}

func (m mapData[DT]) Keys() (keys []string) {
	keys = make([]string, 0)
	m.data.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			keys = append(keys, string(k))
		}
		return nil
	})
	return
}

func (m mapData[DT]) Range(f func(key string, data DT) error) {
	m.data.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				var data DT
				err := json.Unmarshal(v, &data)
				if err != nil {
					return err
				}
				return f(string(k), data)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *mapData[DT]) Exists(key string) (exists bool) {
	//_, exists = m.data.Load(key)
	return
}

func (m mapData[DT]) createEvent(data DT) (e event.StoreEvent, err error) {
	key := m.getKey(data)
	eventType := event.Create
	if m.Exists(key) {
		eventType = event.Update
	}

	return event.NewBuilder[DT]().
		WithType(eventType).
		WithData(data).
		WithMetadata(event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(key),
		}).
		BuildStore()
}

func (m *mapData[DT]) Delete(data DT) (err error) {
	e, err := event.NewBuilder[DT]().
		WithType(event.Delete).
		WithData(data).
		WithMetadata(event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(m.getKey(data)),
		}).
		BuildStore()

	err = m.esh.SetAndWait(e)
	return
}

func (m *mapData[DT]) Set(data DT) (err error) {
	log.Println("Set and wait start")
	e, err := m.createEvent(data)
	if err != nil {
		return
	}
	err = m.esh.SetAndWait(e)
	log.Println("Set and wait end")
	return
}
