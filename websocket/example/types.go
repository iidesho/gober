package example

import "time"

type PongData struct {
	Data  string        `json:"data"`
	Sleep time.Duration `json:"sleep"`
}
