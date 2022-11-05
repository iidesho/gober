package health

import (
	"net"
	"time"

	log "github.com/cantara/bragi"
)

var Version string
var BuildTime string

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
	Version   string    `json:"version"`
	BuildTime string    `json:"build_time"`
	IP        net.IP    `json:"ip"`
	Since     time.Time `json:"running_since"`
	Now       time.Time `json:"now"`
}

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func (h health) GetHealthReport() Report {
	return Report{
		Status:    "UP",
		Version:   Version,
		BuildTime: BuildTime,
		IP:        h.IP,
		Since:     h.Since,
		Now:       time.Now(),
	}
}
