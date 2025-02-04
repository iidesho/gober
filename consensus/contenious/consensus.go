package contenious

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"sync"

	//	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/discovery"
	"github.com/iidesho/gober/itr"
	gs "github.com/iidesho/gober/sync"
	"github.com/iidesho/gober/webserver"
	"github.com/iidesho/gober/websocket"
)

const MESSAGE_BUFFER_SIZE = 1024

type Consensus interface {
	Connected() []string
	Consents() map[string]string
	Request(uuid.UUID)
	Status(uuid.UUID) (approved bool)
	Abort(uuid.UUID)
	Completed(uuid.UUID)
}

type consensus struct {
	discoverer  discovery.Discoverer
	ctx         context.Context
	consents    map[uuid.UUID]acknowledgements
	distributor chan<- message
	aborted     chan<- uuid.UUID
	mutex       *sync.RWMutex
	connected   []server
	completed   []uuid.UUID // Will grow infinetely, should trim old completed
	selfBytes   []byte
	id          uuid.UUID
}

type server struct {
	w        chan<- websocket.Write[[]byte]
	hostname string
	id       uuid.UUID
}

type messageType uint8

const (
	invalid messageType = iota
	completed
	failed
	request
	decline
	approve
)

type message struct {
	t         messageType
	requester uuid.UUID
	consID    uuid.UUID
	// sender    uuid.UUID
}

type acknowledgements struct {
	acknowledgers gs.Slice[uuid.UUID]
	requester     uuid.UUID
}

func (m message) WriteBytes(w io.Writer) error {
	err := bcts.WriteUInt8(w, m.t)
	if err != nil {
		return err
	}
	err = bcts.WriteStaticBytes(w, m.requester[:])
	if err != nil {
		return err
	}
	/*
		err = bcts.WriteStaticBytes(w, m.sender[:])
		if err != nil {
			return err
		}
	*/
	err = bcts.WriteStaticBytes(w, m.consID[:])
	if err != nil {
		return err
	}
	return nil
}

func (m *message) ReadBytes(r io.Reader) error {
	err := bcts.ReadUInt8(r, &m.t)
	if err != nil {
		return err
	}
	err = bcts.ReadStaticBytes(r, m.requester[:])
	if err != nil {
		return err
	}
	/*
		err = bcts.ReadStaticBytes(r, m.sender[:])
		if err != nil {
			return err
		}
	*/
	err = bcts.ReadStaticBytes(r, m.consID[:])
	if err != nil {
		return err
	}
	return nil
}

func New(
	serv webserver.Server,
	token gs.Obj[string],
	discoverer discovery.Discoverer,
	topic string,
	ctx context.Context,
) (Consensus, <-chan uuid.UUID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, nil, err
	}
	s := serv.Base().Group(topic)
	/*
		s.Use(
			func(c *fiber.Ctx) error {
				auth := webserver.GetAuthHeader(c)
				if strings.TrimSpace(auth) == "" {
					return c.Status(http.StatusUnauthorized).
						JSON(map[string]string{"status": "Unauthorized"})
				}
				if auth != token.Get() {
					return c.Status(http.StatusForbidden).JSON(map[string]string{"status": "FORBIDDEN"})
				}
				return c.Next()
			},
		)
	*/

	selfBytes, err := bcts.Write(bcts.String(discoverer.Self()))
	if err != nil {
		return nil, nil, err
	}

	aborted := make(chan uuid.UUID, MESSAGE_BUFFER_SIZE)
	distributor := make(chan message, MESSAGE_BUFFER_SIZE)
	c := consensus{
		id:          id,
		connected:   []server{},                       // sync.NewSlice[server](),
		consents:    map[uuid.UUID]acknowledgements{}, // sync.NewMap[uuid.UUID, acknowledgements](),
		mutex:       &sync.RWMutex{},
		distributor: distributor,
		discoverer:  discoverer,
		selfBytes:   selfBytes,
		aborted:     aborted,
		ctx:         ctx,
	}
	go func(distributor chan message, ctx context.Context) {
		ticker := time.NewTicker(
			time.Millisecond*1000 + time.Millisecond*time.Duration(rand.IntN(100)),
		)
		for {
			select {
			case m := <-distributor:
				o, err := bcts.Write(m)
				if sbragi.WithError(err).Error("marshaling response bytes") {
					continue
				}
				c.mutex.RLock()
				for _, s := range c.connected {
					errC := make(chan error, 1)
					s.w <- websocket.Write[[]byte]{Data: o, Err: errC}
					err = <-errC
					if sbragi.WithError(err).Error("writing response bytes to socket") {
						c.connected = itr.NewIterator(c.connected).
							Filter(func(v server) bool { return v.id != s.id }).
							Collect()
						/*
							c.connected.DeleteWhere(func(v server) bool {
								return v.id == s.id
							})
						*/
						continue
					}
				}
				c.mutex.RUnlock()
			case <-ticker.C:
				for s := range itr.NewIterator(c.discoverer.Servers()).Filter(func(s string) bool {
					return c.discoverer.Self() != s
				}).Filter(func(s string) bool {
					c.mutex.RLock()
					defer c.mutex.RUnlock()
					return !itr.NewIterator(c.connected).Contains(func(v server) bool {
						return v.hostname == s
					})
				}) {
					sbragi.Debug("trying to connect", "from", c.discoverer.Self(), "to", s)
					u, err := url.Parse(fmt.Sprintf("ws://%s/%s/consent", s, topic))
					if sbragi.WithError(err).Error("parsing discovered server", "server", s) {
						continue
					}
					r, w, err := websocket.Dial[[]byte](u, ctx)
					if sbragi.WithError(err).
						Info("connecting to server", "server", s, "url", u.String()) {
						continue
					}
					go c.reader(r, w, ctx)

					// sbragi.Info("found server", "server", s)
				}
			case <-ctx.Done():
				close(c.aborted)
				return
			}
		}
	}(distributor, ctx)
	// c.connected.Add(id)

	websocket.ServeFiber(
		s,
		"consent",
		nil,
		func(r <-chan []byte, w chan<- websocket.Write[[]byte], req *http.Request, ctx context.Context) {
			c.reader(r, w, ctx)
		},
	)
	return &c, aborted, nil
}

func (c consensus) Connected() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return itr.NewIterator(c.connected).
		TransformToString(func(s server) (string, error) { return s.hostname, nil }).
		FilterOk().
		Collect()
}

func (c consensus) Consents() map[string]string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	m := map[string]string{}
	for k, v := range c.consents {
		m[k.String()] = strings.Join(
			itr.NewIterator(v.acknowledgers.Slice()).
				TransformToString(func(s uuid.UUID) (string, error) { return s.String(), nil }).
				FilterOk().
				Collect(),
			" ",
		)
	}
	return m
}

func (c *consensus) Request(id uuid.UUID) {
	c.mutex.RLock()
	if itr.NewIterator(c.completed).Contains(func(v uuid.UUID) bool { return v == id }) {
		sbragi.Warning("cannot request completed")
		c.mutex.RUnlock()
		return
	}
	_, ok := c.consents[id]
	c.mutex.RUnlock()
	if ok {
		return // Request exists anc we should error?
	}
	c.mutex.Lock()
	if itr.NewIterator(c.completed).Contains(func(v uuid.UUID) bool { return v == id }) {
		sbragi.Warning("cannot request completed")
		// This should be impossible in the time to aquire a write lock
		c.mutex.Unlock()
		return
	}
	_, ok = c.consents[id]
	if ok {
		c.mutex.Unlock()
		return // Request exists anc we should error?
	}
	accs := gs.NewSlice[uuid.UUID]()
	accs.Add(c.id)
	c.consents[id] = acknowledgements{
		requester:     c.id,
		acknowledgers: accs,
	}
	c.mutex.Unlock()
	/*
		accs, isNew := c.consents.GetOrInit(id, func() acknowledgements {
			return acknowledgements{
				r.Lock()equester:     c.id,
				acknowledgers: sync.NewSlice[uuid.UUID](),
			}
		})
		if !isNew {
			return // Request exists anc we should error?
		}
		accs.acknowledgers.AddUnique(c.id, func(v1, v2 uuid.UUID) bool {
			return v1 == v2
		})
	*/
	c.distributor <- message{
		t:         request,
		requester: c.id,
		// sender:    c.id,
		consID: id,
	}
}

func (c consensus) Status(id uuid.UUID) bool {
	c.mutex.RLock()
	accs, ok := c.consents[id]
	c.mutex.RUnlock()
	if !ok {
		sbragi.Trace("consent does not exist")
		return false
	}
	if accs.requester != c.id {
		sbragi.Trace("we are not requester")
		return false
	}
	sbragi.Trace("checking if we won request", "self", c.id, "accs", accs.acknowledgers.Slice())
	return accs.acknowledgers.Len() > (len(c.discoverer.Servers())-1)/2
}

func (c *consensus) Completed(id uuid.UUID) {
	c.mutex.RLock()
	if itr.NewIterator(c.completed).Contains(func(v uuid.UUID) bool { return v == id }) {
		c.mutex.RUnlock()
		// should log
		return
	}
	accs, ok := c.consents[id]
	c.mutex.RUnlock()
	if !ok {
		sbragi.Warning("consent does not exist")
		return
	}
	if accs.requester != c.id {
		return // Should error
	}
	c.mutex.Lock()
	if itr.NewIterator(c.completed).Contains(func(v uuid.UUID) bool { return v == id }) {
		c.mutex.Unlock()
		// should log
		return
	}
	accs, ok = c.consents[id]
	if !ok {
		sbragi.Warning("consent does not exist")
		c.mutex.Unlock()
		return
	}
	if accs.requester != c.id {
		c.mutex.Unlock()
		return // Should error
	}
	c.completed = append(c.completed, id)
	delete(c.consents, id)
	c.mutex.Unlock()
	c.distributor <- message{
		t:         completed,
		requester: c.id,
		// sender:    c.id,
		consID: id,
	}
}

func (c *consensus) Abort(id uuid.UUID) {
	c.mutex.RLock()
	if itr.NewIterator(c.completed).Contains(func(v uuid.UUID) bool { return v == id }) {
		sbragi.Warning("cannot abort completed")
		c.mutex.RUnlock()
		return
	}
	accs, ok := c.consents[id]
	c.mutex.RUnlock()
	if !ok {
		sbragi.Warning("consent does not exist")
		return
	}
	if accs.requester != c.id {
		return // Should error
	}
	c.mutex.Lock()
	if itr.NewIterator(c.completed).Contains(func(v uuid.UUID) bool { return v == id }) {
		sbragi.Warning("cannot abort completed")
		c.mutex.Unlock()
		return
	}
	accs, ok = c.consents[id]
	if !ok {
		sbragi.Warning("consent does not exist")
		c.mutex.Unlock()
		return
	}
	if accs.requester != c.id {
		c.mutex.Unlock()
		return // Should error
	}
	delete(c.consents, id)
	c.mutex.Unlock()
	c.distributor <- message{
		t:         failed,
		requester: c.id,
		// sender:    c.id,
		consID: id,
	}
	c.aborted <- id
}

func (c *consensus) reader(r <-chan []byte, w chan<- websocket.Write[[]byte], ctx context.Context) {
	defer close(w)
	w <- websocket.Write[[]byte]{Data: append(c.id.Bytes(), c.selfBytes...)}
	read, ok := <-r
	if !ok {
		return
	}
	dByte := bytes.NewReader(read)
	var rid uuid.UUID
	err := bcts.ReadStaticBytes(dByte, rid[:])
	if sbragi.WithError(err).Error("getting node id from connected server") {
		// Shold write error back to connector
		return
	}
	// sbragi.Info("test", "id", rid)
	var rName bcts.String
	err = bcts.ReadString(dByte, &rName)
	if sbragi.WithError(err).
		Error("getting node name from connected server",
			"read", len(read), "id", rid.String()) {
		// Shold write error back to connector
		return
	}
	c.mutex.RLock()
	if itr.NewIterator(c.connected).Contains(func(v server) bool {
		return v.id == rid
	}) {
		c.mutex.RUnlock()
		sbragi.Debug("rejected double connection to server", "name", rName, "id", rid.String())
		return
	}
	c.mutex.RUnlock()
	c.mutex.Lock()
	if itr.NewIterator(c.connected).Contains(func(v server) bool {
		return v.id == rid
	}) {
		c.mutex.Unlock()
		sbragi.Debug("rejected double connection to server", "name", rName, "id", rid.String())
		return
	}
	c.connected = append(c.connected, server{
		id:       rid,
		hostname: string(rName),
		w:        w,
	})
	c.mutex.Unlock()
	/*
		if !c.connected.AddUnique(server{
			id:       rid,
			hostname: string(rName),
			w:        w,
		}, func(v1, v2 server) bool {
			return v1.id == v2.id
		}) {
			sbragi.Info("rejected double connection to server", "name", rName, "id", rid.String())
			return
		}
	*/
	sbragi.Debug(
		"connected to server",
		"self",
		c.discoverer.Self(),
		"name",
		rName,
		"id",
		rid.String(),
	)
	defer func() {
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		c.mutex.Lock()
		defer c.mutex.Unlock()
		c.connected = itr.NewIterator(c.connected).
			Filter(func(s server) bool { return s.id != rid }).
			Collect()
		/*
			c.connected.DeleteWhere(func(v server) bool {
				return v.id == rid
			})
		*/
		for k, v := range c.consents {
			if v.requester == rid {
				delete(c.consents, k)
				sbragi.Info("aborting", "id", k.String(), "consents", v.acknowledgers.Slice())
				c.aborted <- k
				sbragi.Info("aborted", "id", k.String(), "consents", v.acknowledgers.Slice())
				continue
			}
			v.acknowledgers.DeleteWhere(func(v uuid.UUID) bool { return v == rid })
		}
	}()

	// Create a reader function that takes in a channel to a distributor routine and the ID and Name of the resver and lastly the reader. Then thes routine should read until completion.
	// The distributor routine will close the writer on write error.
	// Reader functeon will defer cleanup of the connetion
	// Requests for consensus will be sent to the distributor
	// The distributor can also periodically call the discovery function to find new or remove old nodes. Select on ctx, time and distributor channel
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.ctx.Done():
			return
		case m, ok := <-r:
			if !ok {
				return
			}
			d, err := bcts.Read[message](m)
			if sbragi.WithError(err).
				Error("could not read data from consensus producer") {
				continue
			}
			switch d.t {
			case approve:
				sbragi.Trace(
					"received approve request",
					"self",
					c.id.String(),
					"sender",
					rid,
					"id",
					d.consID,
				)
				/*
					accs, _ := c.consents.GetOrInit(d.consID, func() acknowledgements {
						return acknowledgements{
							requester:     d.requester,
							acknowledgers: sync.NewSlice[uuid.UUID](),
						}
					})
				*/
				c.mutex.RLock()
				accs, ok := c.consents[d.consID]
				if ok && accs.requester != d.requester {
					c.mutex.RUnlock()
					// should log
					continue
				}
				c.mutex.RUnlock()
				c.mutex.Lock()
				accs, ok = c.consents[d.consID]
				if ok && accs.requester != d.requester {
					c.mutex.Unlock()
					// should log
					continue
				}
				if !ok {
					accs = acknowledgements{
						requester:     d.requester,
						acknowledgers: gs.NewSlice[uuid.UUID](),
					}
				}
				c.mutex.Unlock()
				accs.acknowledgers.AddUnique(rid, func(v1, v2 uuid.UUID) bool {
					return v1 == v2
				})
				// Should distribute our approval
				if accs.acknowledgers.AddUnique(c.id, func(v1, v2 uuid.UUID) bool {
					return v1 == v2
				}) {
					c.distributor <- *d
				}
			case decline:
				sbragi.Trace(
					"received decline request",
					"self",
					c.id.String(),
					"sender",
					rid,
					"id",
					d.consID,
				)
				c.mutex.RLock()
				accs, ok := c.consents[d.consID]
				if !ok || d.requester != accs.requester {
					c.mutex.RUnlock()
					// should log
					continue
				}
				c.mutex.RUnlock()
				/*
					accs, ok := c.consents.Get(d.consID)
					if !ok {
						continue
					}
				*/
				accs.acknowledgers.DeleteWhere(
					func(v uuid.UUID) bool { return v == rid },
				)
				if accs.acknowledgers.Len() == 1 {
					accs.acknowledgers.DeleteWhere(
						func(v uuid.UUID) bool { return v == c.id },
					)
				}
				if accs.acknowledgers.Len() == 0 {
					c.mutex.Lock()
					accs, ok := c.consents[d.consID]
					if !ok || d.requester != accs.requester {
						c.mutex.Unlock()
						// should log
						continue
					}
					if accs.acknowledgers.Len() != 0 {
						c.mutex.Unlock()
						// should log
						continue
					}
					delete(c.consents, d.consID)
					c.mutex.Unlock()
					// c.consents.Delete(d.consID)
				}
			case failed:
				sbragi.Trace(
					"received failed request",
					"self",
					c.id.String(),
					"sender",
					rid,
					"id",
					d.consID,
				)
				if rid != d.requester {
					// log error
					sbragi.Warning(
						"sender is not requester",
						"sender",
						rid,
						"requester",
						d.requester,
					)
					continue
				}
				c.mutex.RLock()
				accs, ok := c.consents[d.consID]
				if !ok {
					sbragi.Warning(
						"failed non existing consensus",
						"id",
						d.consID,
					)
					c.mutex.RUnlock()
					continue
				}
				if accs.requester != d.requester {
					// log error
					sbragi.Warning(
						"requester is not the one we approved",
						"requester",
						d.requester,
						"appoved",
						accs.requester,
					)
					c.mutex.RUnlock()
					continue
				}
				c.mutex.RUnlock()
				c.mutex.Lock()
				accs, ok = c.consents[d.consID]
				if !ok {
					sbragi.Warning(
						"failed non existing consensus",
						"id",
						d.consID,
					)
					c.mutex.Unlock()
					continue
				}
				if accs.requester != d.requester {
					// log error
					sbragi.Warning(
						"requester is not the one we approved",
						"requester",
						d.requester,
						"appoved",
						accs.requester,
					)
					c.mutex.Unlock()
					continue
				}
				delete(c.consents, d.consID)
				sbragi.Debug("deleted", "consents", c.consents)
				c.mutex.Unlock()
				c.aborted <- d.consID
				continue
			case request:
				sbragi.Trace(
					"received request request",
					"self",
					c.id.String(),
					"sender",
					rid,
					"id",
					d.consID,
				)
				if rid != d.requester {
					// log error
					sbragi.Warning(
						"sender is not requester",
						"sender",
						rid,
						"requester",
						d.requester,
					)
					continue
				}
				// Doing it lacy without read checking
				c.mutex.Lock()
				if itr.NewIterator(c.completed).
					Contains(func(v uuid.UUID) bool { return v == d.consID }) {
					c.mutex.Unlock()
					continue
				}
				accs, ok := c.consents[d.consID]
				if !ok {
					accs = acknowledgements{
						requester:     d.requester,
						acknowledgers: gs.NewSlice[uuid.UUID](),
					}
					accs.acknowledgers.Add(d.requester)
					c.consents[d.consID] = accs
				}
				c.mutex.Unlock()
				/*
					accs, _ := c.consents.GetOrInit(d.consID, func() acknowledgements {
						return acknowledgements{
							requester:     d.requester,
							acknowledgers: sync.NewSlice[uuid.UUID](),
						}
					})
				*/
				// If accs was deleted in other thread, then this is technically not thread safe, should consider implementing a synd package with transactions
				if accs.requester != d.requester {
					d.t = decline
					c.distributor <- *d
					continue
				}
				accs.acknowledgers.AddUnique(c.id, func(v1, v2 uuid.UUID) bool {
					return v1 == v2
				})
				d.t = approve
				c.distributor <- *d
			case completed:
				sbragi.Trace(
					"received request request",
					"self",
					c.id.String(),
					"sender",
					rid,
					"id",
					d.consID,
				)
				if rid != d.requester {
					// log error
					sbragi.Warning(
						"sender is not requester",
						"sender",
						rid,
						"requester",
						d.requester,
					)
					continue
				}
				// Doing it lacy without read checking
				c.mutex.Lock()
				_, ok := c.consents[d.consID]
				if !ok {
					// consent is not pressent, should log
					continue
				}
				delete(c.consents, d.consID)
				c.completed = append(c.completed, d.consID)
				c.mutex.Unlock()
			default:
				// log error
				sbragi.Warning("unsupported message type", "type", d.t)
			}
		}
	}
}
