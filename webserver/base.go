package webserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cantara/gober/webserver/health"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"net/http"
	"os"
)

var Name string

const (
	CONTENT_TYPE      = "Content-Type"
	CONTENT_TYPE_JSON = "application/json"
	AUTHORIZATION     = "Authorization"
)

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

func UnmarshalBody[bodyT any](c *gin.Context) (v bodyT, err error) {
	var unmarshalErr *json.UnmarshalTypeError
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&v)
	if err != nil {
		if errors.As(err, &unmarshalErr) {
			err = fmt.Errorf("wrong type provided for \"%s\" should be of type (%s) but got value {%s} after reading %d",
				unmarshalErr.Field, unmarshalErr.Type, unmarshalErr.Value, unmarshalErr.Offset)
		}
		return
	}
	return
}

func ErrorResponse(c *gin.Context, message string, httpStatusCode int) {
	resp := make(map[string]string)
	resp["error"] = message
	c.JSON(httpStatusCode, resp)
}

func GetAuthHeader(c *gin.Context) (header string) {
	headers := c.Request.Header[AUTHORIZATION]
	if len(headers) > 0 {
		header = headers[0]
	}
	return
}
