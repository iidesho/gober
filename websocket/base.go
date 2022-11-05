package websocket

import (
	"context"
	log "github.com/cantara/bragi"
	"github.com/gin-gonic/gin"
	"nhooyr.io/websocket"
)

func Websocket(r *gin.RouterGroup, path string, wsfunc func(ctx context.Context, conn *websocket.Conn) bool) {
	r.GET(path, func(c *gin.Context) {
		s, err := websocket.Accept(c.Writer, c.Request, nil)
		if err != nil {
			log.AddError(err).Fatal("while accepting websocket")
		}
		defer s.Close(websocket.StatusNormalClosure, "") //Could be smart to do something here to fix / tell people of errors.
		for wsfunc(c.Request.Context(), s) {
		}
	})
}
