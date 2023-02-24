package websocket

type Write[T any] struct {
	Data T            `json:"data"`
	Err  chan<- error `json:"err"`
}
