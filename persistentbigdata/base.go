package persistentbigmap

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/gofiber/fiber/v2"
	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	jsoniter "github.com/json-iterator/go"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/crypto"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/consumer"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/webserver"
)

var json = jsoniter.ConfigDefault

type EventMap[DT, MT any] interface {
	Get(key string) (data DT, err error)
	Len() (l int)
	Keys() (keys []string)
	Range(f func(key string, data DT) error)
	Delete(data MT) (err error)
	Set(data DT, meta MT) (err error)
	Exists(key string) (exists bool)
}

type transactionCheck struct{}

type mapData[DT, MT any] struct {
	es                consumer.Consumer[bcts.Bytes, *bcts.Bytes]
	server            webserver.Server
	ctx               context.Context
	getKey            func(d MT) string
	provider          stream.CryptoKeyProvider
	discoveryProvider stream.CryptoKeyProvider
	data              *badger.DB
	transactionChan   chan transactionCheck
	dataTypeVersion   string
	dataTypeName      string
	discoveryPath     string
	positionKey       []byte
	instance          uuid.UUID
}

type metadata[MT any] struct {
	Data  MT        `json:"data"`
	OldId uuid.UUID `json:"old_id"`
	NewId uuid.UUID `json:"new_id"`
}

type action uint8

const (
	invalid action = iota
	update
	query
	response
)

type discoveryMetadata[MT any] struct {
	Key      string `json:"key"`
	Endpoint string `json:"endpoint"`
	Meta     metadata[MT]
	Instance uuid.UUID `json:"instance"`
	Action   action    `json:"action"`
}

func Init[DT, MT any](
	serv webserver.Server,
	s stream.Stream,
	dataTypeName, dataTypeVersion string,
	p stream.CryptoKeyProvider,
	getKey func(d MT) string,
	ctx context.Context,
) (ed EventMap[DT, MT], err error) {
	dir := "./eventmap/" + dataTypeName
	os.MkdirAll(dir, 0750)
	db, err := badger.Open(badger.DefaultOptions(dir).
		WithMaxTableSize(1024 * 1024 * 8).
		WithValueLogFileSize(1024 * 1024 * 8).
		WithValueLogLoadingMode(options.FileIO))
	if err != nil {
		return
	}
	instance, err := uuid.NewV7()
	if err != nil {
		return
	}
	m := mapData[DT, MT]{
		data:              db,
		transactionChan:   make(chan transactionCheck),
		dataTypeName:      dataTypeName,
		dataTypeVersion:   dataTypeVersion,
		instance:          instance,
		provider:          p,
		discoveryProvider: stream.StaticProvider("SkajsFNVOEV81k824LMxO1XCNi+mSpw+HtxKc/e+Xp4="),
		getKey:            getKey,
		discoveryPath:     fmt.Sprintf("/persistentbigdata/%s/:key", dataTypeName),
		server:            serv,
		positionKey:       []byte(fmt.Sprintf("%s_%s_position", s.Name(), dataTypeName)),
		ctx:               ctx,
	}

	serv.Base().Get(m.discoveryPath, func(c *fiber.Ctx) error {
		keyStr := c.Params("key")
		if keyStr == "" {
			return webserver.ErrorResponse(c, "key not provided", http.StatusNotFound)
		}
		key, err := uuid.FromString(keyStr)
		if err != nil {
			webserver.ErrorResponse(c, "key needs to be a uuid", http.StatusBadRequest)
		}
		var dataByte []byte
		err = m.data.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key.Bytes())
			if err != nil {
				return err
			}

			dataByte, err = item.ValueCopy(nil)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return webserver.ErrorResponse(c, err.Error(), http.StatusNotFound)
			}
			sbragi.WithError(err).Error("while getting data for sync request")
			return webserver.ErrorResponse(
				c,
				"internal server error",
				http.StatusInternalServerError,
			)
		}
		var data DT
		err = json.Unmarshal(dataByte, &data)
		if err != nil {
			sbragi.WithError(err).Error("while unmarshalling get request for big data")
			return webserver.ErrorResponse(c, "json umarshal error", http.StatusInternalServerError)
		}
		return c.Status(http.StatusOK).JSON(data)
	})

	from := store.STREAM_START
	_ = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(m.positionKey)
		if err != nil {
			return err
		}
		var pos uint64
		err = item.Value(func(val []byte) error {
			pos = binary.LittleEndian.Uint64(val)
			return nil
		})
		if err != nil {
			return err
		}
		from = store.StreamPosition(pos)
		return nil
	})
	es, err := consumer.New[bcts.Bytes](s, m.provider, m.ctx)
	if err != nil {
		return
	}
	m.es = es

	eventChan, err := es.Stream(event.AllTypes(), from, stream.ReadDataType(dataTypeName), ctx)
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-eventChan:
				func() {
					defer e.Acc()
					if e.Type == event.Deleted {
						m.delete(e.ReadEvent)
						return
					}
					m.create(e.ReadEvent)
				}()
			}
		}
	}()

	ed = &m
	return
}

func (m *mapData[DT, MT]) create(e event.ReadEvent[bcts.Bytes, *bcts.Bytes]) {
	var dmd discoveryMetadata[MT]
	err := json.Unmarshal(*e.Data, &dmd)
	if sbragi.WithError(err).Error("unmarshaling eventmap data") {
		return
	}
	if dmd.Action == query {
		if dmd.Instance == m.instance {
			return
		}
		err := m.data.View(func(txn *badger.Txn) error {
			_, err := txn.Get(dmd.Meta.NewId.Bytes())
			return err
		})
		if err != nil {
			return
		}
		u := m.server.Url()
		u.Path = u.Path + strings.ReplaceAll(m.discoveryPath, ":key", dmd.Meta.NewId.String())
		b, err := json.Marshal(discoveryMetadata[MT]{
			Key:      dmd.Key,
			Endpoint: u.String(),
			Instance: m.instance,
			Meta:     dmd.Meta,
			Action:   response,
		})
		if sbragi.WithError(err).Error("marshaling event") {
			return
		}

		db := bcts.Bytes(b)
		es := event.Event[bcts.Bytes, *bcts.Bytes]{
			Type: event.Updated,
			Data: &db,
			Metadata: event.Metadata{
				Version:  m.dataTypeVersion,
				DataType: m.dataTypeName,
				Key:      crypto.SimpleHash(dmd.Meta.NewId.String()),
				Extra:    map[bcts.TinyString]bcts.SmallBytes{"instance": m.instance.Bytes()},
			},
		}
		we := event.NewWriteEvent(es)
		m.es.Write() <- we
		<-we.Done() // Missing error
		/*
			_, err = m.es.Store(es)
			if err != nil {
				return
			}
		*/
		return
	}
	if dmd.Instance != m.instance {
		err := m.data.View(func(txn *badger.Txn) error {
			_, err := txn.Get(dmd.Meta.NewId.Bytes())
			return err
		})
		if err == nil {
			sbragi.Debug("data already stored")
			return
		}
		_, raw, err := externalImage[DT](dmd.Endpoint)
		if err != nil {
			sbragi.WithError(err).
				Warning("while getting updated big data")
				// Should probably use tasks to verify completions instead.
			b, err := json.Marshal(discoveryMetadata[MT]{
				Key:      dmd.Key,
				Instance: m.instance,
				Meta:     dmd.Meta,
				Action:   query,
			})
			if sbragi.WithError(err).Error("marshaling event") {
				return
			}

			db := bcts.Bytes(b)
			es := event.Event[bcts.Bytes, *bcts.Bytes]{
				Type: event.Updated,
				Data: &db,
				Metadata: event.Metadata{
					Version:  m.dataTypeVersion,
					DataType: m.dataTypeName,
					Key:      crypto.SimpleHash(dmd.Meta.NewId.String()),
					Extra:    map[bcts.TinyString]bcts.SmallBytes{"instance": m.instance.Bytes()},
				},
			}
			we := event.NewWriteEvent(es)
			m.es.Write() <- we
			<-we.Done() // Missing error
			/*
				_, err = m.es.Store(es)
				if err != nil {
					log.WithError(err).Error("unable to store query event after get miss")
					return
				}
			*/
			return
		}
		err = m.data.Update(func(txn *badger.Txn) error {
			return txn.Set(dmd.Meta.NewId.Bytes(), raw)
		})
		if err != nil {
			sbragi.WithError(err).Warning("Update error")
			return
		}
	}
	data, err := json.Marshal(dmd.Meta)
	if err != nil {
		sbragi.WithError(err).Warning("Update error")
		return
	}
	err = m.data.Update(func(txn *badger.Txn) error {
		err = txn.Set([]byte(m.getKey(dmd.Meta.Data)), data)
		if err != nil {
			return err
		}
		if e.Type == event.Updated {
			err = txn.Delete(dmd.Meta.OldId.Bytes())
			if err != nil {
				return err
			}
		}

		pos := make([]byte, 8)
		binary.LittleEndian.PutUint64(pos, e.Position)
		return txn.Set(m.positionKey, pos)
	})
	if err != nil {
		sbragi.WithError(err).Warning("Update error")
		return
	}
}

func (m *mapData[DT, MT]) delete(e event.ReadEvent[bcts.Bytes, *bcts.Bytes]) {
	var dmd discoveryMetadata[MT]
	err := json.Unmarshal(*e.Data, &dmd)
	if sbragi.WithError(err).Error("unmarshaling eventmap data") {
		return
	}
	err = m.data.Update(func(txn *badger.Txn) error {
		err := txn.Delete(dmd.Meta.OldId.Bytes())
		if err != nil {
			return err
		}
		err = txn.Delete([]byte(m.getKey(dmd.Meta.Data)))
		if err != nil {
			return err
		}

		pos := make([]byte, 8)
		binary.LittleEndian.PutUint64(pos, e.Position)
		return txn.Set(m.positionKey, pos)
	})
	if err != nil {
		sbragi.WithError(err).Warning("Delete error")
	}
}

var ErrKeyNotFound = fmt.Errorf("provided key does not exist")

func (m *mapData[DT, MT]) Get(key string) (data DT, err error) {
	var ed []byte
	err = m.data.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		edt, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		var md metadata[MT]
		err = json.Unmarshal(edt, &md)
		if err != nil {
			return err
		}
		item, err = txn.Get(md.NewId.Bytes())
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
		if errors.Is(err, badger.ErrKeyNotFound) {
			err = ErrKeyNotFound
			return
		}
		return
	}

	err = json.Unmarshal(ed, &data)
	if err != nil {
		return
	}
	return
}

func (m *mapData[DT, MT]) Len() (l int) {
	return len(m.Keys())
}

func (m *mapData[DT, MT]) Keys() (keys []string) {
	keys = make([]string, 0)
	err := m.data.View(func(txn *badger.Txn) error {
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
	if err != nil {
		sbragi.WithError(err).Error("while reading bigdata disc store")
		return
	}
	return
}

func (m *mapData[DT, MT]) Range(f func(key string, data DT) error) {
	err := m.data.View(func(txn *badger.Txn) error {
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
	if err != nil {
		sbragi.WithError(err).Error("while reading bigdata disc store")
		return
	}
}

func (m *mapData[DT, MT]) Exists(key string) (exists bool) {
	_, err := m.Get(key)
	return err == nil
}

func (m *mapData[DT, MT]) Delete(data MT) (err error) {
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

	var md metadata[MT]
	err = json.Unmarshal(ed, &m)
	if err != nil {
		return
	}

	b, err := json.Marshal(discoveryMetadata[MT]{
		Meta: md,
	})
	if err != nil {
		return err
	}

	db := bcts.Bytes(b)
	e := event.Event[bcts.Bytes, *bcts.Bytes]{
		Type: event.Deleted,
		Data: &db,
		Metadata: event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(md.NewId.String()),
		},
	}

	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() // Missing error
	/*
		_, err = m.es.Store(e)
	*/
	return
}

func (m *mapData[DT, MT]) Set(data DT, meta MT) (err error) {
	sbragi.Trace("Set and wait start")
	newId, err := uuid.NewV7()
	if err != nil {
		return
	}
	eventType := event.Created
	md := metadata[MT]{
		NewId: newId,
		Data:  meta,
	}
	key := m.getKey(meta)

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
	var smd metadata[MT]
	if err == nil {
		err = json.Unmarshal(ed, &smd)
	}
	if err == nil {
		eventType = event.Updated
		md.OldId = smd.NewId
	}
	u := m.server.Url()
	u.Path = u.Path + strings.ReplaceAll(m.discoveryPath, ":key", md.NewId.String())
	dmd := discoveryMetadata[MT]{
		Key:      key,
		Endpoint: u.String(),
		Instance: m.instance,
		Meta:     md,
		Action:   update,
	}
	b, err := json.Marshal(dmd)
	if err != nil {
		return err
	}

	db := bcts.Bytes(b)
	e := event.Event[bcts.Bytes, *bcts.Bytes]{
		Type: eventType,
		Data: &db,
		Metadata: event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(md.NewId.String()),
			Extra:    map[bcts.TinyString]bcts.SmallBytes{"instance": m.instance.Bytes()},
		},
	}
	d, err := json.Marshal(data)
	if err != nil {
		return
	}
	sbragi.Debug("publishing updated data", "endpoint", dmd.Endpoint, "id", md.NewId.String())
	err = m.data.Update(func(txn *badger.Txn) error {
		return txn.Set(md.NewId.Bytes(), d)
	})
	if err != nil {
		return
	}
	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() // Missing error
	/*
		_, err = m.es.Store(e)
		if err != nil {
			return
		}
	*/
	return
}

func externalImage[DT any](url string) (d DT, raw []byte, err error) {
	resp, err := http.Get(url)
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("get miss")
		return
	}
	defer func() {
		err := resp.Body.Close()
		if err != nil {
			sbragi.WithError(err).Debug("while closing response body")
		}
	}()
	raw, err = io.ReadAll(resp.Body)
	if err == nil {
		return
	}
	err = json.Unmarshal(raw, &d)
	return
}
