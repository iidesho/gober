package persistentbigmap

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/gofrs/uuid"

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

type metadata struct {
	OldId uuid.UUID `json:"old_id"`
	NewId uuid.UUID `json:"new_id"`
}

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, getKey func(dt DT) string, ctx context.Context) (ed EventMap[DT], err error) {
	db, err := badger.Open(badger.DefaultOptions("./eventmap/" + dataTypeName))
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
		getKey: func(d DT) string {
			return "metadata_" + getKey(d)
		},
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
	eventChan, err := stream.NewStream[metadata](s, event.AllTypes(), from, stream.ReadDataType(dataTypeName), p, ctx)
	if err != nil {
		return
	}
	m.esh = stream.InitSetHelper(func(e event.Event[metadata]) {
		data, err := json.Marshal(e.Data)
		if err != nil {
			return
		}
		err = db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(e.Data.NewId.Bytes()))
			if err != nil {
				return err
			}
			bd, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var d DT
			err = json.Unmarshal(bd, &d)
			if err != nil {
				return err
			}
			err = txn.Set([]byte(getKey(d)), data)
			if err != nil {
				return err
			}
			if e.Type == event.Update {
				err = txn.Delete([]byte(e.Data.OldId.Bytes()))
				if err != nil {
					return err
				}
			}

			pos := make([]byte, 8)
			binary.LittleEndian.PutUint64(pos, e.Position)
			return txn.Set(positionKey, pos)
		})
		if err != nil {
			log.AddError(err).Warning("Update error")
		}
	}, func(e event.Event[metadata]) {
		err := db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(e.Data.OldId.Bytes()))
			if err != nil {
				return err
			}
			if err != nil {
				return err
			}
			bd, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var d DT
			err = json.Unmarshal(bd, &d)
			if err != nil {
				return err
			}

			err = txn.Delete([]byte(e.Data.OldId.Bytes()))
			if err != nil {
				return err
			}
			err = txn.Delete([]byte(getKey(d)))
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

func (m *mapData[DT]) Delete(data DT) (err error) {
	var ed []byte
	err = m.data.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(m.getKey(data)))
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

	var md metadata
	err = json.Unmarshal(ed, &m)
	if err != nil {
		return
	}
	e, err := event.NewBuilder[metadata]().
		WithType(event.Delete).
		WithData(md).
		WithMetadata(event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(md.NewId.String()),
		}).
		BuildStore()

	err = m.esh.SetAndWait(e)
	return
}

func (m *mapData[DT]) Set(data DT) (err error) {
	log.Println("Set and wait start")
	newId, err := uuid.NewV7()
	if err != nil {
		return
	}
	eventType := event.Create
	md := metadata{
		NewId: newId,
	}

	var ed []byte
	err = m.data.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(m.getKey(data)))
		if err != nil {
			return err
		}

		ed, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	var smd metadata
	if err == nil {
		err = json.Unmarshal(ed, &smd)
	}
	if err == nil {
		eventType = event.Update
		md.OldId = smd.NewId
	}

	e, err := event.NewBuilder[metadata]().
		WithType(eventType).
		WithData(md).
		WithMetadata(event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(md.NewId.String()),
		}).
		BuildStore()
	if err != nil {
		return
	}

	err = m.esh.SetAndWait(e)
	log.Println("Set and wait end")
	return
}
