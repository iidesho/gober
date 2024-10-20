package consensus

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofrs/uuid"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/discovery"
	"github.com/iidesho/gober/sync"
	"github.com/iidesho/gober/webserver"
	"github.com/iidesho/gober/webserver/health"
)

type builder struct {
	serv webserver.Server

	server
}

type ConsBuilderFunc func(topic string, consTimeout time.Duration) (cons Consensus, err error)

type ConsensusBuilder interface {
	AddTopic(topic string, consTimeout time.Duration) (cons Consensus, err error)
	Run()
}

func Init(port uint16, token string, discoverer discovery.Discoverer) (ConsensusBuilder, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	serv, err := webserver.Init(port, true)
	if err != nil {
		return nil, err
	}
	serv.API().Use(
		func(c *fiber.Ctx) error {
			auth := webserver.GetAuthHeader(c)
			if strings.TrimSpace(auth) == "" {
				return c.Status(http.StatusUnauthorized).
					JSON(map[string]string{"status": "Unauthorized"})
			}
			if auth != token {
				return c.Status(http.StatusForbidden).JSON(map[string]string{"status": "FORBIDDEN"})
			}
			return c.Next()
		},
	)
	c := builder{
		serv: serv,
		server: server{
			port:  port,
			token: token,
			id:    id.String(),
			disc:  discoverer,
		},
	}

	return c, nil
}

func Data(c *fiber.Ctx, code int, d any) error {
	log.Debug("adding repsons data in correct format", "format", c.Accepts())
	return c.Status(code).JSON(d)
}

// Solve this with a websocket instead
func (producer builder) AddTopic(t string, consTimeout time.Duration) (out Consensus, err error) {
	cons := consensus{
		server: producer.server,
		reqs:   sync.NewMap[sync.Stack[topic, *topic]](),

		timeout: consTimeout,
		topic:   t,
	}
	producer.serv.API().Get(fmt.Sprintf("/%s/status", t), func(c *fiber.Ctx) error {
		return Data(c, http.StatusOK, cons.disc.Servers())
	})
	//Get satus of a consent i know about
	producer.serv.API().Get(fmt.Sprintf("/%s/:id/consent", t), func(c *fiber.Ctx) error {
		v, ok := cons.reqs.Get(bcts.TinyString(c.Params("id")[0:]))
		if !ok {
			return Data(
				c,
				http.StatusBadRequest,
				map[string]string{"status": "missing consent request"},
			)
		}
		return Data(c, http.StatusOK, v)
	})
	//Request consent from me
	producer.serv.API().Post(fmt.Sprintf("/%s/:id/consent", t), func(c *fiber.Ctx) error {
		id := c.Params("id")[0:]
		reqs, isNew := cons.reqs.GetOrInit(bcts.TinyString(id), sync.NewStack[topic, *topic])
		if !isNew {
			v, ok := reqs.Peek()
			if ok && time.Now().After(v.Timeout) {
				status := http.StatusConflict
				if producer.id == v.Requester {
					status = http.StatusOK
				}
				return Data(c, status, consentResponse{
					Topic:     cons.topic,
					Id:        id,
					Requester: v.Requester,
					Consenter: producer.id,
				})
			}
		}
		v, err := webserver.UnmarshalBody[consentRequest](c)
		if err != nil {
			log.WithError(err).Warning("did not bind json")
			return Data(
				c,
				http.StatusBadRequest,
				map[string]string{"status": "could not unmashal data", "error": err.Error()},
			)
		}
		v.Consents = []Consent{
			{
				Id: producer.id,
				Ip: health.GetOutboundIP().String(),
			},
		}
		reqs.Push(&v.topic)
		return Data(c, http.StatusCreated, consentResponse{
			Topic:     v.Topic,
			Id:        v.Id,
			Requester: v.Requester,
			Consenter: producer.id,
		})
	})
	//Has gotten updated conseed info and informs conceed winner(me)
	producer.serv.API().Patch(fmt.Sprintf("/%s/:id/consent", t), func(c *fiber.Ctx) error {
		id := c.Params("id")[0:]
		reqs, ok := cons.reqs.Get(bcts.TinyString(id))
		if !ok {
			return Data(
				c,
				http.StatusNotFound,
				map[string]string{"status": "missing consent request id"},
			)
		}
		v, ok := reqs.Peek()
		if !ok {
			return Data(
				c,
				http.StatusNotFound,
				map[string]string{"status": "missing consent request"},
			)
		}
		data, err := webserver.UnmarshalBody[Consent](c)
		if err != nil {
			return Data(
				c,
				http.StatusBadRequest,
				map[string]string{"status": "could not unmashal data", "error": err.Error()},
			)
		}
		if containsConsent(data.Id, v.Consents) < 0 {
			v.Consents = append(v.Consents, data) //This feels wrong //Is not thread safe
		}
		return Data(c, http.StatusOK, v)
	})
	//Conseed to me or inform me of a conseed
	producer.serv.API().Delete(fmt.Sprintf("/%s/:id/consent", t), func(c *fiber.Ctx) error {
		id := c.Params("id")[0:]
		reqs, ok := cons.reqs.Get(bcts.TinyString(id))
		if !ok {
			return Data(
				c,
				http.StatusNotFound,
				map[string]string{"status": "missing consent request id"},
			)
		}
		v, ok := reqs.Peek()
		if !ok {
			return Data(
				c,
				http.StatusNotFound,
				map[string]string{"status": "missing consent request"},
			)
		}
		data, err := webserver.UnmarshalBody[consentRequest](c)
		if err != nil {
			return Data(
				c,
				http.StatusBadRequest,
				map[string]string{"status": "could not unmashal data", "error": err.Error()},
			)
		}
		if producer.id == v.Requester { //Conseeding to me
			if data.Conseeding == nil {
				return Data(
					c,
					http.StatusBadRequest,
					map[string]string{"status": "not conseeding when reporting conseed"},
				)
			}
			if v.Conseeding != nil && v.Conseeding.Before(*data.Conseeding) {
				return Data(
					c,
					http.StatusConflict,
					map[string]string{"status": "conseeded too late"},
				)
			}
			v.Conseeding = nil //This seems wrong
			//Should probably remove contender

		} else if v.Requester == data.Requester { //Informing me that you have conseeded
			reqs.Pop()
			//cons.reqs.Delete(id)
		}
		return nil
	})
	return &cons, nil
}

func (producer builder) Run() {
	producer.serv.Run()
}
