package store

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/valyala/fasthttp"
)

var log = sbragi.WithLocalScope(sbragi.LevelInfo)

func CreateEndpoint(
	r fiber.Router,
	s stream.Stream,
	path string,
) {
	r.Get(path, func(c *fiber.Ctx) error {
		ctx := c.Context()
		stream, err := s.Stream(store.STREAM_START, ctx)
		if sbragi.WithError(err).Error("opening stream") {
			return err
		}
		c.Set("Content-Type", "text/event-stream")
		c.Set("Cache-Control", "no-cache")
		c.Set("Connection", "keep-alive")
		c.Set("Transfer-Encoding", "chunked")

		c.Status(fiber.StatusOK).
			Context().
			SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
				t := time.NewTicker(time.Second * 5)
				for {
					select {
					case <-ctx.Done():
						return
					case e := <-t.C:
						fmt.Fprintf(w, "ping: %s\n\n", e.Format(time.RFC3339))
						err := w.Flush()
						if err != nil {
							fmt.Printf("Error while flushing: %v. Closing http connection.\n", err)
							return
						}
					case e := <-stream:
						fmt.Fprint(w, "data: ")
						sw := base64.NewEncoder(base64.RawURLEncoding, w)
						err := e.WriteBytes(sw)
						if err != nil {
							sw.Close()
							fmt.Printf("Error BCTS the event: %v. Closing http connection.\n", err)
							return
						}
						err = sw.Close()
						if err != nil {
							fmt.Printf("Error encoding: %v. Closing http connection.\n", err)
							return
						}
						fmt.Fprint(w, "\n\n")
						err = w.Flush()
						if err != nil {
							fmt.Printf("Error while flushing: %v. Closing http connection.\n", err)
							return
						}
					}
				}
			}))

		return nil
	})
}
