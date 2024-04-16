package health

import (
	"net"
	"time"

	log "github.com/iidesho/bragi/sbragi"
)

var Version string
var BuildTime string
var Name string

type health struct {
	IP    net.IP    `json:"ip"`
	Since time.Time `json:"since"`
}

func Init() health {
	return health{
		IP:    GetOutboundIP(),
		Since: time.Now(),
	}
}

type Report struct {
	Status    string    `json:"status"`
	Name      string    `json:"name"`
	Version   string    `json:"version"`
	BuildTime string    `json:"build_time"`
	IP        net.IP    `json:"ip"`
	Since     time.Time `json:"running_since"`
	Now       time.Time `json:"now"`
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
