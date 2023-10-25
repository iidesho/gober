package consensus

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/sync"
	"github.com/cantara/gober/webserver"
)

type Consensus interface {
	Request(id string) bool
	Completed(id string)
}

type consensus struct {
	reqs sync.Map[sync.Stack[*topic]]

	topic   string
	timeout time.Duration

	server
}

func (cons *consensus) Request(id string) bool {
	crs, isNew := cons.reqs.GetOrInit(id, sync.NewStack[*topic])
	if !isNew {
		cr, ok := crs.Peek()
		if ok && (cr.State == Completed || cr.Timeout.After(time.Now())) {
			log.Warning("consensus request already exists", "state", cr.State)
			return false //len(cr.Consents) > len(cons.disc.Servers())/2
		}

	}
	t := topic{
		Requester: cons.id,
		State:     Requested,
		Timeout:   time.Now().Add(cons.timeout),
	}
	cr := consentRequest{
		Topic: cons.topic,
		Id:    id,
		topic: t,
	}
	crs.Push(&t)
	b, _ := json.Marshal(cr) //Should not be able to error
	for _, ip := range cons.disc.Servers() {
		func() {
			req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d/%s/%s/consent", ip, cons.port, cons.topic, id), bytes.NewBuffer(b))
			req.Header.Set(webserver.AUTHORIZATION, cons.token)
			req.Header.Set(webserver.CONTENT_TYPE, webserver.CONTENT_TYPE_JSON)

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				log.WithoutEscalation().WithError(err).Warning("while requesting consent", "topic", cons.topic, "id", id)
				return
			}
			defer resp.Body.Close()
			switch resp.StatusCode {
			case http.StatusBadRequest:
				log.Warning("bad request when requesting consent")
				return
			case http.StatusConflict:
				fallthrough
			case http.StatusCreated:
				fallthrough
			case http.StatusOK:
				var data consentResponse
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					log.WithError(err).Error("while reading request body")
					return
				}
				err = json.Unmarshal(body, &data)
				if err != nil {
					log.WithError(err).Error("while unmarshalling json", "body", string(body))
					return
				}
				if cons.id != data.Requester {
					// Did not consent!
					log.Info("did not get consent")
					return
				}
				if containsConsent(data.Consenter, cr.Consents) >= 0 {
					log.Info("already has consent from consenter")
					return
				}
				cr.Consents = append(cr.Consents, Consent{
					Ip: ip,
					Id: data.Consenter,
				})
				return
			default:
				log.Warning("unexpected consent request status code", "status", resp.Status)
			}
		}()
	}
	crStored, _ := crs.Peek()
	if crStored.Requester != cr.Requester || !crStored.Timeout.Equal(cr.Timeout) {
		log.Info("request state", "stored", crStored, "req", cr, "timeout", crStored.Timeout.Round(time.Microsecond))
		return false
	}
	log.Trace("consent request finished", "consent", cr)
	won := len(cr.Consents) > len(cons.disc.Servers())/2
	crStored.Consents = cr.Consents
	if won {
		crStored.State = Consented
	}
	log.Debug("request state", "stored", crStored, "req", cr)
	//Missing conseed flow
	return won
}

func (cons *consensus) Completed(id string) {
	crs, ok := cons.reqs.Get(id)
	if !ok {
		log.Warning("compleded a non existing consensus", "topic", cons.topic, "id", id)
		return
	}
	c, ok := crs.Peek()
	if !ok {
		log.Warning("compleded a empty consensus", "topic", cons.topic, "id", id)
		return
	}
	//TODO: Need to look at what to do when the newest consensus was not won by completer.
	c.State = Completed
	//TODO: Should inform all other servers of completion
}
