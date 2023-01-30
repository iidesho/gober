package webserver

import (
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/webserver/health"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"strings"

	_ "net/http/pprof"
)

var Name string

const (
	CONTENT_TYPE      = "Content-Type"
	CONTENT_TYPE_JSON = "application/json"
	AUTHORIZATION     = "Authorization"
)

type Server struct {
	r    *gin.Engine
	port uint16
	Base *gin.RouterGroup
	API  *gin.RouterGroup
}

func Init(port uint16) (*Server, error) {
	h := health.Init()
	s := &Server{
		r:    gin.New(),
		port: port,
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
	s.Base = s.r.Group("")
	s.API = s.Base.Group("/" + Name)
	s.API.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, h.GetHealthReport())
	})
	user := os.Getenv("debug.user")
	pass := os.Getenv("debug.pass")
	if user != "" && pass != "" && Name != "" {
		debug := s.API.Group("/debug")
		debug.Use(gin.BasicAuth(gin.Accounts{user: pass}))
		debug.GET("/pprof/*type", func(c *gin.Context) {
			fmt.Println(c.Param("type"))
			switch c.Param("type") {
			case "/profile":
				pprof.Profile(c.Writer, c.Request)
			case "/trace":
				pprof.Trace(c.Writer, c.Request)
			case "/symbol":
				pprof.Symbol(c.Writer, c.Request)
			default:
				pprof.Index(c.Writer, c.Request)
			}
		})
	}
	return s, nil
}

func (s Server) Run() {
	err := s.r.Run(fmt.Sprintf(":%d", s.Port()))
	if err != nil {
		log.AddError(err).Crit("while starting or running webserver")
	}
}

func (s Server) Port() uint16 {
	return s.port
}

func (s Server) Url() (u *url.URL) {
	u = &url.URL{}
	u.Scheme = "http"
	u.Host = fmt.Sprintf("%s:%d", health.GetOutboundIP(), s.Port())
	u.Path = strings.TrimSuffix(s.Base.BasePath(), "/")
	return
}

func UnmarshalBody[bodyT any](c *gin.Context) (v bodyT, err error) {
	if c.GetHeader(CONTENT_TYPE) != CONTENT_TYPE_JSON {
		err = ErrIncorrectContentType
		return
	}
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

var ErrIncorrectContentType = fmt.Errorf("http header did not contain key %s with value %s", CONTENT_TYPE, CONTENT_TYPE_JSON)
