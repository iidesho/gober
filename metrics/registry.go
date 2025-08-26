package metrics

import (
	"os"

	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/webserver/health"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	log      = sbragi.WithLocalScope(sbragi.LevelInfo)
	Registry *prometheus.Registry
)

func Init() {
	Registry = prometheus.NewRegistry()
}

func Push(url string) {
	// Create a pusher. The job name is required.
	// The instance label is optional but highly recommended to distinguish instances.
	pusher := push.New(url, health.Name)

	// Add your custom registry to the pusher.
	// You can also add the default registry if you have other metrics there:
	// pusher.Gatherer(prometheus.DefaultGatherer)
	pusher.Gatherer(Registry)

	// Add grouping labels if you want to categorize metrics further on the Pushgateway.
	// For example, if you have multiple instances of the same job.
	hn, err := os.Hostname()
	if !log.WithError(err).Error("getting hostname for metrics push") {
		pusher.Grouping("instance", hn)
	}
}
