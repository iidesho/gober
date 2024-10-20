package health

import (
	"encoding/json"
	"io"
	"net"
	"strings"
	"time"

	log "github.com/iidesho/bragi/sbragi"
)

var Version string
var BuildTime string
var Name string

type health struct {
	Since time.Time `json:"since"`
	IP    net.IP    `json:"ip"`
	hrj   []byte
	hrns  uint16
	hrne  uint16
}

func Init() health {
	h := health{
		IP:    GetOutboundIP(),
		Since: time.Now(),
	}
	hr := h.GetHealthReport()
	h.hrj, _ = json.Marshal(hr)

	bv, _ := hr.Now.MarshalJSON()
	h.hrns = uint16(strings.Index(string(h.hrj), string(bv)) + 1)
	h.hrne = h.hrns + uint16(len(bv)-2)
	return h
}

type Report struct {
	Since     time.Time `json:"running_since"`
	Now       time.Time `json:"now"`
	Status    string    `json:"status"`
	Name      string    `json:"name"`
	Version   string    `json:"version"`
	BuildTime string    `json:"build_time"`
	IP        net.IP    `json:"ip"`
}

var ip net.IP

func GetOutboundIP() net.IP {
	if ip != nil {
		return ip
	}
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.WithError(err).Error("unable to get outbound ip")
		return nil
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip = localAddr.IP

	return ip
}

func (h health) WriteHealthReport(w io.Writer) error {
	_, err := w.Write(h.hrj[:h.hrns])
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(time.Now().Format(time.RFC3339)))
	if err != nil {
		return err
	}
	_, err = w.Write(h.hrj[h.hrne:])
	if err != nil {
		return err
	}
	return nil
}

func (h health) GetHealthReport() Report {
	return Report{
		Status:    "UP",
		Name:      Name,
		Version:   Version,
		BuildTime: BuildTime,
		IP:        h.IP,
		Since:     h.Since,
		Now:       time.Now(),
	}
}
