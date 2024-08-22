package persistenteventmapttl

import (
	"context"
	"fmt"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/storage"
	"github.com/iidesho/gober/stream/consumer"
	jsoniter "github.com/json-iterator/go"

	"github.com/iidesho/bragi/sbragi"

	"github.com/iidesho/gober/crypto"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
)

var json = jsoniter.ConfigDefault

type transactionCheck struct {
}

type mapData[DT any] struct {
	data            storage.PosStorage[DT]
	transactionChan chan transactionCheck
	dataTypeName    string
	dataTypeVersion string
	provider        stream.CryptoKeyProvider
	es              consumer.Consumer[bcts.Bytes, *bcts.Bytes]
	ctx             context.Context
	getKey          func(dt DT) string
}

func Init[DT any](
	s stream.Stream,
	dataTypeName, dataTypeVersion string,
	p stream.CryptoKeyProvider,
	getKey func(dt DT) string,
	ctx context.Context,
) (ed EventMap[DT], err error) {
	path := "./eventmap/" + dataTypeName
	from := store.STREAM_START
	positionKey := fmt.Sprintf("%s_%s_position", s.Name(), dataTypeName)
	db := storage.NewWPos[DT](path, positionKey)
	pos, err := db.GetUInt64()
	if err == nil {
		from = store.StreamPosition(pos)
	}
	es, err := consumer.New[bcts.Bytes, *bcts.Bytes](s, p, ctx)
	if err != nil {
		return
	}
	m := mapData[DT]{
		data:            db,
		transactionChan: make(chan transactionCheck),
		dataTypeName:    dataTypeName,
		dataTypeVersion: dataTypeVersion,
		provider:        p,
		es:              es,
		getKey:          getKey,
	}
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
				var data DT
				err := json.Unmarshal(*e.Data, &data)
				if sbragi.WithError(err).Error("unmarshaling eventmap data") {
					continue
				}
				if e.Type == event.Deleted {
					if sbragi.WithError(db.Delete(getKey(data), storage.OptPos(e.Position))).
						Error("delete") {
						continue
					}
					e.Acc()
				}
				if sbragi.WithError(db.Set(getKey(data), data, storage.OptPos(e.Position))).
					Error("update") {
					continue
				}
				e.Acc()
			}
		}
	}()

	ed = &m
	return
}

func (m *mapData[DT]) Stream(
	eventTypes []event.Type,
	from store.StreamPosition,
	filter stream.Filter,
	ctx context.Context,
) (out <-chan event.Event[bcts.Bytes, *bcts.Bytes], err error) {
	s, err := m.es.Stream(eventTypes, from, filter, ctx)
	if err != nil {
		return
	}
	c := make(chan event.Event[bcts.Bytes, *bcts.Bytes])
	go func() {
		for e := range s {
			select {
			case <-ctx.Done():
				return
			case c <- e.Event:
				continue
			}
		}
	}()
	out = c
	return
}

var ERROR_KEY_NOT_FOUND = fmt.Errorf("provided key does not exist")

func (m *mapData[DT]) Get(key string) (data DT, err error) {
	return m.data.Get(key)
}

func (m *mapData[DT]) Exists(key string) (exists bool) {
	_, err := m.Get(key)
	return err == nil
}

func (m *mapData[DT]) createEvent(data DT) (e event.Event[bcts.Bytes, *bcts.Bytes], err error) {
	key := m.getKey(data)
	eventType := event.Created
	if m.Exists(key) {
		eventType = event.Updated
	}
	b, err := json.Marshal(data)
	if err != nil {
		return event.Event[bcts.Bytes, *bcts.Bytes]{}, err
	}

	db := bcts.Bytes(b)
	e = event.Event[bcts.Bytes, *bcts.Bytes]{
		Type: eventType,
		Data: &db,
		Metadata: event.Metadata{
			Version:  m.dataTypeVersion,
			DataType: m.dataTypeName,
			Key:      crypto.SimpleHash(key),
		},
	}
	return
}

func (m *mapData[DT]) Delete(data DT) (err error) {
	b, err := json.Marshal(data)
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
			Key:      crypto.SimpleHash(m.getKey(data)),
		},
	}

	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	return
}

func (m *mapData[DT]) Set(data DT) (err error) {
	sbragi.Trace("Set and wait start")
	e, err := m.createEvent(data)
	if err != nil {
		return
	}
	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	sbragi.Trace("Set and wait end")
	return
}
