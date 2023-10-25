package consensus

import (
	"strings"
	"time"

	"github.com/cantara/gober/discovery"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

type ReqFunc func(id string) bool

type server struct {
	port  uint16
	token string
	id    string
	disc  discovery.Discoverer
}

type State string

const (
	Invalid   State = "invalid"
	Requested State = "requested"
	Consented State = "consented"
	Completed State = "completed"
	Timedout  State = "timedout"
)

type topic struct {
	Requester  string     `json:"requester" xml:"requester,attr" html:"requester"`
	Consents   []Consent  `json:"consents" xml:"consents" html:"consents"`
	State      State      `json:"state" xml:"state,attr" html:"state"`
	Timeout    time.Time  `json:"timeout" xml:"timeout,attr" html:"timeout"`
	Conseeding *time.Time `json:"conseeding,omitempty" xml:"conseeding" html:"conseeding"`
}

type consentRequest struct {
	Topic string `json:"topic" xml:"topic,attr" html:"topic"`
	Id    string `json:"id" xml:"id,attr" html:"id"`

	topic
}

type consentResponse struct {
	Topic     string `json:"topic" xml:"topic,attr" html:"topic"`
	Id        string `json:"id" xml:"id,attr" html:"id"`
	Requester string `json:"requester" xml:"requester,attr" html:"requester"`
	Consenter string `json:"consenter" xml:"consenter,attr" html:"consenter"`
}

type Consent struct {
	Id string
	Ip string
}

func contains(s string, arr []string) bool {
	s = strings.ToLower(s)
	for _, v := range arr {
		if s == strings.ToLower(v) {
			return true
		}
	}
	return false
}

func containsConsentReq(id string, creqs []consentRequest) int {
	for i, v := range creqs {
		if v.Id == id {
			return i
		}
	}
	return -1
}

func containsConsent(id string, creqs []Consent) int {
	for i, v := range creqs {
		if v.Id == id {
			return i
		}
	}
	return -1
}
