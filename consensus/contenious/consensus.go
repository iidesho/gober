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
	Request(ConsID)
	Status(ConsID) Status
	Abort(ConsID)
	Completed(ConsID)
}

type (
	NodeID uuid.UUID
	ConsID uuid.UUID
)

func (id NodeID) String() string {
	return uuid.UUID(id).String()
}

func (id ConsID) String() string {
	return uuid.UUID(id).String()
}

func (id NodeID) Bytes() []byte {
	return uuid.UUID(id).Bytes()
}

func (id ConsID) Bytes() []byte {
	return uuid.UUID(id).Bytes()
}

type consensus struct {
	discoverer  discovery.Discoverer
	ctx         context.Context
	requests    map[ConsID]gs.Slice[req]
	distributor chan<- message
	aborted     chan<- ConsID
	approvedC   chan<- ConsID
	mutex       *sync.RWMutex
	connected   []server
	completed   []ConsID // Will grow infinetely, should trim old completed
	approved    map[ConsID]NodeID
	selfBytes   []byte
	id          NodeID
}

type server struct {
	w        chan<- websocket.Write[[]byte]
	hostname string
	id       NodeID
}

type messageType uint8

const (
	invalid messageType = iota
	completed
	failed
	request
	decline
	approve
	giveup
)

type Status uint8

const (
	StatusInvalid Status = iota
	StatusCompleted
	StatusFailed
	StatusInProgress
	StatusApproved
)

type message struct {
	t         messageType
	requester NodeID
	consID    ConsID
	// sender    uuid.UUID
}

type req struct {
	acknowledgers gs.Slice[NodeID]
	decliners     gs.Slice[NodeID]
	requester     NodeID
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
) (Consensus, <-chan ConsID, <-chan ConsID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, nil, nil, err
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
		return nil, nil, nil, err
	}

	aborted := make(chan ConsID, MESSAGE_BUFFER_SIZE)
	approved := make(chan ConsID, MESSAGE_BUFFER_SIZE)
	distributor := make(chan message, MESSAGE_BUFFER_SIZE)
	c := consensus{
		id:          NodeID(id),
		connected:   []server{},                 // sync.NewSlice[server](),
		requests:    map[ConsID]gs.Slice[req]{}, // sync.NewMap[consID, gs.Slice[req]{}](),
		approved:    map[ConsID]NodeID{},
		mutex:       &sync.RWMutex{},
		distributor: distributor,
		discoverer:  discoverer,
		selfBytes:   selfBytes,
		aborted:     aborted,
		approvedC:   approved,
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
	return &c, aborted, approved, nil
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
	for k, v := range c.requests {
		m[k.String()] = strings.Join(
			itr.NewIterator(v.Slice()).
				TransformToString(func(s req) (string, error) {
					return s.requester.String() +
						": acknowlegers[" +
						strings.Join(
							itr.NewIterator(s.acknowledgers.Slice()).
								TransformToString(func(s NodeID) (string, error) {
									return s.String(), nil
								}).
								FilterOk().
								Collect(),
							" ") +
						"] decliners[" +
						strings.Join(
							itr.NewIterator(s.decliners.Slice()).
								TransformToString(func(s NodeID) (string, error) {
									return s.String(), nil
								}).
								FilterOk().
								Collect(),
							" ") +
						"]", nil
				}).
				FilterOk().
				Collect(),
			" ")
	}
	return m
}

func (c *consensus) Request(id ConsID) {
	c.mutex.RLock()
	if itr.NewIterator(c.completed).Contains(func(v ConsID) bool { return v == id }) {
		sbragi.Warning("cannot request completed")
		c.mutex.RUnlock()
		return
	}
	if itr.NewMapKeysIterator(c.approved).Contains(func(v ConsID) bool { return v == id }) {
		sbragi.Warning("cannot request approved")
		c.mutex.RUnlock()
		return
	}
	reqs, ok := c.requests[id]
	c.mutex.RUnlock()
	if ok && reqs.ReadItr().
		Filter(func(s req) bool {
			return s.acknowledgers.Contains(s.requester) >= 0
		}).
		Count() >= 0 {
		return // Valid request exists, we don't need to request again
	}
	c.mutex.Lock()
	if itr.NewIterator(c.completed).Contains(func(v ConsID) bool { return v == id }) {
		sbragi.Warning("cannot request completed")
		// This should be impossible in the time to aquire a write lock
		c.mutex.Unlock()
		return
	}

	if itr.NewMapKeysIterator(c.approved).Contains(func(v ConsID) bool { return v == id }) {
		sbragi.Warning("cannot request approved")
		c.mutex.Unlock()
		return
	}
	reqs, ok = c.requests[id]
	if ok && reqs.ReadItr().
		Filter(func(s req) bool {
			return s.acknowledgers.Contains(s.requester) >= 0
		}).
		Count() >= 0 {
		c.mutex.Unlock()
		return // Valid request exists, we don't need to request again
	}
	if len(c.discoverer.Servers()) == 1 {
		c.approved[id] = c.id
		c.approvedC <- id
		c.mutex.Unlock()
		return // only 1 node, no need to send request
	}
	accs := gs.NewSlice[NodeID]()
	decs := gs.NewSlice[NodeID]()
	accs.Add(c.id)
	if !ok {
		reqs = gs.NewSlice[req]()
	}
	reqs.Add(req{
		requester:     c.id,
		acknowledgers: accs,
		decliners:     decs,
	})
	c.requests[id] = reqs

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

func (c consensus) Status(id ConsID) Status {
	c.mutex.RLock()
	if itr.NewIterator(c.completed).Contains(func(v ConsID) bool { return v == id }) {
		c.mutex.RUnlock()
		return StatusCompleted
	}
	if itr.NewMapKeysIterator(c.approved).Contains(func(v ConsID) bool { return v == id }) {
		c.mutex.RUnlock()
		return StatusApproved
	}
	_, ok := c.requests[id]
	c.mutex.RUnlock()
	if !ok {
		sbragi.Trace("consent does not exist")
		return StatusInvalid
	} else {
		return StatusInProgress
	}
}

func (c *consensus) Completed(id ConsID) {
	c.mutex.RLock()
	if itr.NewIterator(c.completed).Contains(func(v ConsID) bool { return v == id }) {
		c.mutex.RUnlock()
		// should log
		return
	}
	if _, ok := c.requests[id]; ok {
		c.mutex.RUnlock()
		sbragi.Warning("consent not approved yet")
		return
	}
	requester, ok := c.approved[id]
	c.mutex.RUnlock()
	if !ok {
		sbragi.Warning("consent does not exist")
		return
	}
	if requester != c.id {
		sbragi.Warning("we didn't win request") // should error
		return
	}
	c.mutex.Lock()
	if itr.NewIterator(c.completed).Contains(func(v ConsID) bool { return v == id }) {
		c.mutex.Unlock()
		// should log
		return
	}
	if _, ok := c.requests[id]; ok {
		c.mutex.Unlock()
		sbragi.Warning("consent not approved yet")
		return
	}
	requester, ok = c.approved[id]
	if !ok {
		sbragi.Warning("consent does not exist")
		c.mutex.Unlock()
		return
	}
	if requester != c.id {
		sbragi.Warning("we didn't win request") // should error
		c.mutex.Unlock()
		return
	}
	c.completed = append(c.completed, id)
	delete(c.approved, id)
	c.mutex.Unlock()
	c.distributor <- message{
		t:         completed,
		requester: c.id,
		// sender:    c.id,
		consID: id,
	}
}

// this requires you have gotten the approval, if it is still in election process and you want to abort it, there must be something strange happened
func (c *consensus) Abort(id ConsID) {
	c.mutex.RLock()
	if itr.NewIterator(c.completed).Contains(func(v ConsID) bool { return v == id }) {
		sbragi.Warning("cannot abort completed")
		c.mutex.RUnlock()
		return
	}
	_, ok := c.requests[id]
	if ok {
		sbragi.Warning("cannot abort while election in progress")
		c.mutex.RUnlock()
		return
	}
	requester, ok := c.approved[id]
	c.mutex.RUnlock()
	if !ok {
		sbragi.Warning("consent does not exist")
		return
	}
	if ok && requester != c.id {
		sbragi.Warning("we didn't win request") // should error
		return
	}
	c.mutex.Lock()
	if itr.NewIterator(c.completed).Contains(func(v ConsID) bool { return v == id }) {
		sbragi.Warning("cannot abort completed")
		c.mutex.Unlock()
		return
	}
	_, ok = c.requests[id]
	if ok {
		sbragi.Warning("cannot abort while election in progress")
		c.mutex.Unlock()
		return
	}
	requester, ok = c.approved[id]
	if !ok {
		sbragi.Warning("consent does not exist")
		c.mutex.Unlock()
		return
	}
	if ok && requester != c.id {
		sbragi.Warning("we didn't win request") // should error
		c.mutex.Unlock()
		return
	}
	delete(c.approved, id)

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
	var rid NodeID
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
	c.mutex.RLock()
	for cons := range itr.NewMapKeysIterator(c.requests).Filter(func(s ConsID) bool {
		for req := range c.requests[s].ReadItr() {
			if req.requester == c.id {
				return true
			}
		}
		return false
	}) {
		c.distributor <- message{
			t:         request,
			requester: c.id,
			// sender:    c.id,
			consID: cons,
		}
	}
	c.mutex.RUnlock()
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
		for k, v := range c.approved {
			if v == rid {
				delete(c.approved, k)
				sbragi.Info("aborting", "id", k.String())
				c.aborted <- k
				sbragi.Info("aborted", "id", k.String())
				continue
			}
		}
		for k, reqs := range c.requests {
			for _, req := range reqs.Slice() {
				req.acknowledgers.DeleteWhere(func(v NodeID) bool { return v == rid })
				req.decliners.DeleteWhere(func(v NodeID) bool { return v == rid })
			}
			hasack := false
			reqs.DeleteWhere(func(v req) bool {
				if v.requester == rid {
					if v.acknowledgers.Contains(c.id) >= 0 && v.acknowledgers.Contains(rid) >= 0 {
						hasack = true
					}
					return true
				}
				return false
			})
			if reqs.Len() == 0 {
				delete(c.requests, k)
				go c.Request(k)
				continue
			}
			if reqs.ReadItr().
				Filter(func(s req) bool {
					return s.acknowledgers.Contains(s.requester) >= 0
				}).Count() == 0 {
				go c.Request(k)
				continue
			}
			if hasack { // if we are not aknowledger of their requests, we can skip next step
				// always decline current approved one before approve another one
				c.distributor <- message{
					t:         decline,
					requester: rid,
					// sender:    c.id,
					consID: k,
				}
			}
			if reqs.ReadItr().Contains(func(v req) bool {
				return v.acknowledgers.ReadItr().Contains(func(v NodeID) bool {
					return v == c.id
				})
			}) {
				continue
			}

			// get highest voted candidate and approve it
			n := 0
			top := req{}
			for r := range reqs.ReadItr() {
				if r.acknowledgers.Len() > n {
					n = r.acknowledgers.Len()
					top = r
				}
			}
			top.acknowledgers.AddUnique(c.id, func(v1, v2 NodeID) bool { return v1 == v2 })
			top.decliners.DeleteWhere(func(v NodeID) bool { return v == c.id })

			if top.acknowledgers.Len() > (len(c.discoverer.Servers())-1)/2 &&
				top.acknowledgers.Contains(top.requester) >= 0 {
				delete(c.requests, k)
				c.approved[k] = top.requester
				if top.requester == c.id {
					c.approvedC <- k
				}
			}

			c.distributor <- message{
				t:         approve,
				requester: top.requester,
				// sender:    c.id,
				consID: k,
			}

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
				if itr.NewIterator(c.completed).
					Contains(func(v ConsID) bool { return v == d.consID }) {
					c.mutex.RUnlock()
					continue
				}
				_, ok := c.approved[d.consID]
				if ok {
					c.mutex.RUnlock()
					continue
				}
				reqs, ok := c.requests[d.consID]
				c.mutex.RUnlock()

				if ok {
					if d.requester == c.id && reqs.ReadItr().
						Filter(func(s req) bool {
							return s.requester == d.requester
						}).Count() == 0 {
						continue // we have already given up this request
					}

					if reqs.ReadItr().
						Filter(func(s req) bool {
							return s.acknowledgers.Contains(rid) >= 0
						}).Count() > 0 {
						continue // they have approved some request
					}
				}
				c.mutex.Lock()
				if itr.NewIterator(c.completed).
					Contains(func(v ConsID) bool { return v == d.consID }) {
					c.mutex.Unlock()
					continue
				}
				_, ok = c.approved[d.consID]
				if ok {
					c.mutex.Unlock()
					continue
				}
				reqs, ok = c.requests[d.consID]
				if d.requester == c.id && (!ok || reqs.ReadItr().
					Filter(func(s req) bool {
						return s.requester == d.requester
					}).Count() == 0) {
					c.mutex.Unlock()
					continue // we have already given up this request
				}

				if ok && reqs.ReadItr().
					Filter(func(s req) bool {
						return s.acknowledgers.Contains(rid) >= 0
					}).Count() > 0 {
					c.mutex.Unlock()
					sbragi.Fatal(
						"recieved second approval",
						"cons",
						d.consID,
						"from",
						rid,
					)
					continue // they have approved some request
				}
				if ok {
					exist := false
					approve := false
					for req := range itr.NewIterator(reqs.Slice()).
						Filter(func(s req) bool {
							if s.requester == d.requester {
								exist = true
								return true
							}
							return false
						}) {
						req.acknowledgers.AddUnique(
							rid,
							func(v1, v2 NodeID) bool { return v1 == v2 },
						)
						if req.acknowledgers.Len() > (len(c.discoverer.Servers())-1)/2 &&
							req.acknowledgers.Contains(req.requester) >= 0 {
							approve = true
							break
						}
						req.decliners.DeleteWhere(func(v NodeID) bool { return v == rid })
					}
					if approve { // if more than half approved, move request to approved list
						c.approved[d.consID] = d.requester
						delete(c.requests, d.consID)
						c.mutex.Unlock()
						if d.requester == c.id {
							c.approvedC <- d.consID
						}
						continue
					}
					if !exist {
						newreq := req{
							requester:     d.requester,
							acknowledgers: gs.NewSlice[NodeID](),
							decliners:     gs.NewSlice[NodeID](),
						}
						newreq.acknowledgers.Add(rid)
						reqs.Add(newreq)
					}
				} else {
					// Should distribute our approval when it is the first time we get this request
					c.distributor <- *d

					if len(c.discoverer.Servers()) <= 3 { // there will be more than half approval
						c.approved[d.consID] = d.requester
						c.mutex.Unlock()
						continue
					}
					newreq := req{
						requester:     d.requester,
						acknowledgers: gs.NewSlice[NodeID](),
						decliners:     gs.NewSlice[NodeID](),
					}
					newreq.acknowledgers.Add(rid)
					newreq.acknowledgers.Add(c.id)

					c.requests[d.consID] = gs.NewSlice[req]()
					c.requests[d.consID].Add(newreq)
				}
				c.mutex.Unlock()
			case decline:
				//
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
				if itr.NewIterator(c.completed).
					Contains(func(v ConsID) bool { return v == d.consID }) {
					c.mutex.RUnlock()
					continue
				}
				_, ok := c.approved[d.consID]
				if ok {
					c.mutex.RUnlock()
					continue
				}
				reqs, ok := c.requests[d.consID]
				c.mutex.RUnlock()
				if d.requester == c.id && reqs.ReadItr().
					Filter(func(s req) bool {
						return s.requester == d.requester
					}).Count() == 0 {
					continue // we have already given up this request
				}

				c.mutex.Lock()
				if itr.NewIterator(c.completed).
					Contains(func(v ConsID) bool { return v == d.consID }) {
					c.mutex.Unlock()
					continue
				}
				_, ok = c.approved[d.consID]
				if ok {
					c.mutex.Unlock()
					continue
				}
				reqs, ok = c.requests[d.consID]
				if d.requester == c.id && (!ok || reqs.ReadItr().
					Filter(func(s req) bool {
						return s.requester == d.requester
					}).Count() == 0) {
					c.mutex.Unlock()
					continue // we have already given up this request
				}
				if ok {
					exist := false
					givingup := false
					for req := range itr.NewIterator(reqs.Slice()).
						Filter(func(s req) bool {
							if s.requester == d.requester {
								exist = true
								return true
							}
							return false
						}) {
						req.decliners.AddUnique(
							rid,
							func(v1, v2 NodeID) bool { return v1 == v2 },
						)
						if req.requester == c.id &&
							req.decliners.Len() > (len(c.discoverer.Servers())-1)/2 {
							// we can only give up by ourselves
							givingup = true
							break
						}
						req.acknowledgers.DeleteWhere(func(v NodeID) bool { return v == rid })
					}
					if givingup { // if more than half declined, we will give up
						delete(c.requests, d.consID)
						c.mutex.Unlock()
						c.distributor <- message{
							t:         giveup,
							requester: c.id,
							// sender:    c.id,
							consID: d.consID,
						}
						continue
					}
					if !exist {
						newreq := req{
							requester:     d.requester,
							acknowledgers: gs.NewSlice[NodeID](),
							decliners:     gs.NewSlice[NodeID](),
						}
						newreq.decliners.Add(rid)
						reqs.Add(newreq)
					}
				} else {
					// shouldn't be our request then, since we checked before
					newreq := req{
						requester:     d.requester,
						acknowledgers: gs.NewSlice[NodeID](),
						decliners:     gs.NewSlice[NodeID](),
					}
					newreq.decliners.Add(rid)

					c.requests[d.consID] = gs.NewSlice[req]()
					c.requests[d.consID].Add(newreq)
				}
				c.mutex.Unlock()
			case giveup:
				// check if completed & approved
				// check if I have ack him, if so I need to decline him and approve another
				// delete him from request
				// if request empty or (no valid exist) I need to request
				sbragi.Trace(
					"received giveup request",
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
				if itr.NewIterator(c.completed).
					Contains(func(v ConsID) bool { return v == d.consID }) {
					c.mutex.RUnlock()
					continue
				}
				_, ok := c.approved[d.consID]
				if ok {
					c.mutex.RUnlock()
					continue
				}
				reqs, ok := c.requests[d.consID]
				c.mutex.RUnlock()
				if !ok {
					// should log
					continue
				}
				hasack := false
				reqs.DeleteWhere(func(v req) bool {
					if v.requester == rid {
						if v.acknowledgers.Contains(c.id) >= 0 {
							hasack = true
						}
						return true
					}
					return false
				})
				if reqs.Len() == 0 {
					delete(c.requests, d.consID)
					go c.Request(d.consID)
					continue
				}
				if itr.NewIterator(reqs.Slice()).
					Filter(func(s req) bool {
						return s.acknowledgers.Contains(s.requester) >= 0
					}).Count() == 0 {
					go c.Request(d.consID)
					continue
				}
				if !hasack { // if we are not aknowledger of their requests, we can skip next step
					continue
				}
				// always decline current approved one before approve another one
				c.distributor <- message{
					t:         decline,
					requester: rid,
					// sender:    c.id,
					consID: d.consID,
				}

				// get highest voted candidate and approve it
				n := 0
				top := req{}
				for r := range reqs.ReadItr() {
					if r.acknowledgers.Len() > n {
						n = r.acknowledgers.Len()
						top = r
					}
				}
				top.acknowledgers.AddUnique(c.id, func(v1, v2 NodeID) bool { return v1 == v2 })
				top.decliners.DeleteWhere(func(v NodeID) bool { return v == c.id })

				c.mutex.Lock()
				if top.acknowledgers.Len() > (len(c.discoverer.Servers())-1)/2 &&
					top.acknowledgers.Contains(top.requester) >= 0 {
					delete(c.requests, d.consID)
					c.approved[d.consID] = top.requester
					if top.requester == c.id {
						c.approvedC <- d.consID
					}
				}
				c.mutex.Unlock()

				c.distributor <- message{
					t:         approve,
					requester: top.requester,
					// sender:    c.id,
					consID: d.consID,
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
				node, ok := c.approved[d.consID]
				if !ok {
					sbragi.Warning(
						"failed non existing consensus",
						"id",
						d.consID,
					)
					c.mutex.RUnlock()
					continue
				}
				if node != d.requester {
					// log error
					sbragi.Warning(
						"requester is not the one we approved",
						"requester",
						d.requester,
						"appoved",
						node,
					)
					c.mutex.RUnlock()
					continue
				}
				c.mutex.RUnlock()
				c.mutex.Lock()
				node, ok = c.approved[d.consID]
				if !ok {
					sbragi.Warning(
						"failed non existing consensus",
						"id",
						d.consID,
					)
					c.mutex.Unlock()
					continue
				}
				if node != d.requester {
					// log error
					sbragi.Warning(
						"requester is not the one we approved",
						"requester",
						d.requester,
						"appoved",
						node,
					)
					c.mutex.Unlock()
					continue
				}
				delete(c.approved, d.consID)
				sbragi.Debug("deleted", "consents", c.requests)
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
				// check if we have approved other -> decline them
				// approve them, check if more than half approval -> move to approved
				// Doing it lacy without read checking
				c.mutex.Lock()
				if itr.NewIterator(c.completed).
					Contains(func(v ConsID) bool { return v == d.consID }) {
					c.mutex.Unlock()
					continue
				}
				if _, ok := c.approved[d.consID]; ok {
					c.mutex.Unlock()
					continue
				}
				reqs, ok := c.requests[d.consID]

				if ok && reqs.ReadItr().Filter(func(s req) bool {
					return s.requester != rid && s.acknowledgers.Contains(c.id) >= 0
				}).Count() > 0 {
					// decline

					c.mutex.Unlock()
					d.t = decline
					c.distributor <- *d
					continue
				}

				// approve
				if !ok {
					c.requests[d.consID] = gs.NewSlice[req]()
					reqs = c.requests[d.consID]
				}
				ok = false
				acks := 2
				for r := range reqs.WriteItr().Filter(func(s *req) bool { return s.requester == rid }) {
					r.acknowledgers.AddUnique(rid, func(v1, v2 NodeID) bool { return v1 == v2 })
					r.acknowledgers.AddUnique(
						c.id,
						func(v1, v2 NodeID) bool { return v1 == v2 },
					)
					ok = true
					acks = r.acknowledgers.Len()
				}
				if acks > (len(c.discoverer.Servers())-1)/2 { // no need to check requester in acknowlegers here
					delete(c.requests, d.consID)
					c.approved[d.consID] = rid
				}
				if !ok {
					newreq := req{
						requester:     rid,
						acknowledgers: gs.NewSlice[NodeID](),
						decliners:     gs.NewSlice[NodeID](),
					}
					newreq.acknowledgers.Add(rid)
					newreq.acknowledgers.Add(c.id)
					reqs.Add(newreq)
				}

				c.mutex.Unlock()
				d.t = approve
				c.distributor <- *d
			case completed:
				sbragi.Trace(
					"received completed request",
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
				_, ok := c.approved[d.consID]
				if ok {
					delete(c.approved, d.consID)
					c.completed = append(c.completed, d.consID)
					c.mutex.Unlock()
					continue
				}
				sbragi.Warning(
					"consent not found in approved list",
					"consent",
					d.consID,
					"node",
					c.id,
				)
				_, ok = c.requests[d.consID]
				if ok {
					delete(c.requests, d.consID)
					c.completed = append(c.completed, d.consID)
					c.mutex.Unlock()
					continue
				}
				sbragi.Warning(
					"consent not found in approved list nor request list",
					"consent",
					d.consID,
					"node",
					c.id,
				)
				c.mutex.Unlock()
			default:
				// log error
				sbragi.Warning("unsupported message type", "type", d.t)
			}
		}
	}
}
