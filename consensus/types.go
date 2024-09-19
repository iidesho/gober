package consensus

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/discovery"
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
	Requester  string     `json:"requester"            xml:"requester,attr" html:"requester"`
	Consents   []Consent  `json:"consents"             xml:"consents"       html:"consents"`
	State      State      `json:"state"                xml:"state,attr"     html:"state"`
	Timeout    time.Time  `json:"timeout"              xml:"timeout,attr"   html:"timeout"`
	Conseeding *time.Time `json:"conseeding,omitempty" xml:"conseeding"     html:"conseeding"`
}

func (t topic) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0)) //Version
	if err != nil {
		return
	}
	err = bcts.WriteTinyString(w, t.Requester)
	if err != nil {
		return
	}
	err = bcts.WriteSlice(w, t.Consents)
	if err != nil {
		return
	}
	err = bcts.WriteTinyString(w, t.State)
	if err != nil {
		return
	}
	err = bcts.WriteTime(w, t.Timeout)
	if err != nil {
		return
	}
	var hasCons uint8
	if t.Conseeding != nil {
		hasCons = 1
	}
	err = bcts.WriteUInt8(w, hasCons)
	if err != nil {
		return
	}
	err = bcts.WriteTime(w, *t.Conseeding)
	if err != nil {
		return
	}
	return nil
}

func (t *topic) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid topic version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadTinyString(r, &t.Requester)
	if err != nil {
		return
	}
	//err = bcts.ReadSlice(r, &t.Consents)
	if err != nil {
		return
	}
	err = bcts.ReadTinyString(r, &t.State)
	if err != nil {
		return
	}
	err = bcts.ReadTime(r, &t.Timeout)
	if err != nil {
		return
	}
	var hasCons uint8
	err = bcts.ReadUInt8(r, &hasCons)
	if err != nil {
		return
	}
	if hasCons == 1 {
		err = bcts.ReadTime(r, t.Conseeding)
		if err != nil {
			return
		}
	}
	return nil
}

type consentRequest struct {
	Topic string `json:"topic" xml:"topic,attr" html:"topic"`
	Id    string `json:"id"    xml:"id,attr"    html:"id"`

	topic
}

type consentResponse struct {
	Topic     string `json:"topic"     xml:"topic,attr"     html:"topic"`
	Id        string `json:"id"        xml:"id,attr"        html:"id"`
	Requester string `json:"requester" xml:"requester,attr" html:"requester"`
	Consenter string `json:"consenter" xml:"consenter,attr" html:"consenter"`
}

type Consent struct {
	Id string
	Ip string
}

func (c Consent) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0)) //Version
	if err != nil {
		return
	}
	err = bcts.WriteTinyString(w, c.Id)
	if err != nil {
		return
	}
	err = bcts.WriteTinyString(w, c.Ip)
	if err != nil {
		return
	}
	return nil
}

func (c *Consent) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid consent version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadTinyString(r, &c.Id)
	if err != nil {
		return
	}
	err = bcts.ReadTinyString(r, &c.Ip)
	if err != nil {
		return
	}
	return nil
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
