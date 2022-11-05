package webserver

import (
	"github.com/cantara/gober/webserver/health"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
)

var Name string

type Server struct {
	r   *gin.Engine
	API *gin.RouterGroup
}

func Init() *Server {
	h := health.Init()
	s := &Server{
		r: gin.New(),
	}
	if Name == "" {
		s.r.Use(gin.LoggerWithConfig(gin.LoggerConfig{
			SkipPaths: []string{"/health"},
		}))
	} else {
		s.r.Use(gin.LoggerWithConfig(gin.LoggerConfig{
			SkipPaths: []string{"/" + Name + "/health"},
		}))
	}
	s.r.Use(gin.Recovery())
	config := cors.DefaultConfig()
	config.AllowOrigins = []string{"*"}
	s.r.Use(cors.New(config))
	base := s.r.Group("/" + Name)
	s.API = base.Group("")
	s.API.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, h.GetHealthReport())
	})
	return s
}

func (s Server) Run() {
	s.r.Run(":" + os.Getenv("webserver.port"))
}
