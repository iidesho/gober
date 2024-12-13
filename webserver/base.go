package webserver

import (
	stdJson "encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"runtime/debug"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/gofiber/fiber/v2/middleware/basicauth"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/earlydata"
	"github.com/iidesho/bragi"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/webserver/health"
	"github.com/joho/godotenv"
	jsoniter "github.com/json-iterator/go"
)

const (
	CONTENT_TYPE      = "Content-Type"
	CONTENT_TYPE_JSON = "application/json"
	AUTHORIZATION     = "Authorization"
)

var json = jsoniter.Config{
	IndentionStep:                 0,
	MarshalFloatWith6Digits:       true,
	EscapeHTML:                    true,
	SortMapKeys:                   false,
	UseNumber:                     true,
	DisallowUnknownFields:         true,
	OnlyTaggedField:               true,
	ValidateJsonRawMessage:        true,
	ObjectFieldMustBeSimpleString: false,
	CaseSensitive:                 false,
}.Froze()

func init() {
	err := godotenv.Load("local_override.properties")
	if err != nil {
		sbragi.WithoutEscalation().WithError(err).
			Debug("Error loading local_override.properties file", "file", "local_override.properties")
		err = godotenv.Load(".env")
		if err != nil {
			sbragi.WithoutEscalation().
				WithError(err).
				Debug("Error loading .env file", "file", ".env")
		}
	}
	logDir := os.Getenv("log.dir")
	if logDir != "" {
		bragi.SetPrefix(health.Name)
		handler, err := sbragi.NewHandlerInFolder(logDir)
		if err != nil {
			sbragi.WithError(err).Fatal("Unable to sett logdir", "path", logDir)
		}
		handler.MakeDefault()
		sbragiger, err := sbragi.NewLogger(&handler)
		if err != nil {
			sbragi.WithError(err).Fatal("Unable create new logger", "handler", handler)
		}
		sbragiger.SetDefault()
		//defer handler.Cancel()    removed because it should run for the entirety of the program and will be cleaned up by the os. Reff: Russ Cox comments on AtExit
	}
	if os.Getenv("debug.port") != "" {
		go func() {
			sbragi.WithError(http.ListenAndServe(":"+os.Getenv("debug.port"), nil)).
				Info("while running debug server", "port", os.Getenv("debug.port"))
		}()
	}
}

type Server interface {
	Base() fiber.Router
	API() fiber.Router
	Run()
	Port() uint16
	Url() (u *url.URL)
}

type server struct {
	r    *fiber.App
	base fiber.Router
	api  fiber.Router
	port uint16
}

func Init(port uint16, from_base bool) (Server, error) {
	h := health.Init()
	s := server{
		r: fiber.New(fiber.Config{
			//Prefork:           true,
			AppName:           health.Name,
			StreamRequestBody: true,
			EnablePrintRoutes: true,
			JSONDecoder:       json.Unmarshal,
			JSONEncoder:       json.Marshal,
			// Override default error handler
			ErrorHandler: func(c *fiber.Ctx, err error) error {
				// Status code defaults to 500
				status := http.StatusInternalServerError //default error status

				// Retrieve the custom status code if it's a *fiber.Error
				var e *fiber.Error
				if errors.As(err, &e) {
					status = e.Code
				}
				msg := map[string]interface{}{
					"status":      status,
					"status_text": http.StatusText(status),
					"error_msg":   err.Error(),
				}

				c.Set("Content-Type", "application/json")
				err = c.Status(status).JSON(msg)
				if err != nil {
					// In case the SendFile fails
					return c.Status(fiber.StatusInternalServerError).
						SendString("Internal Server Error")
				}

				// Return from handler
				return nil
			},
		}),
		port: port,
	}
	s.r.Use(earlydata.New())

	healthPath := "/health"
	if !from_base && health.Name != "" {
		healthPath = "/" + health.Name + "/health"
	}
	s.r.Use(func(c *fiber.Ctx) error {

		if string(c.Context().Path()) == healthPath {
			return c.Next()
		}
		//start := time.Now()
		err := c.Next()
		/*
			sbragi.WithError(err).
				Info(fmt.Sprintf("[%s]%s", c.Route().Method, c.Route().Path), "duration", time.Since(start), "ip", c.IP(), "ips", c.IPs(), "hostname", c.Hostname())
		*/
		return err
	})
	s.r.Use(compress.New(compress.Config{Level: compress.LevelBestSpeed}))
	s.r.Use(func(c *fiber.Ctx) (err error) {
		defer func() {
			r := recover()
			if r != nil {
				err = errors.Join(
					err,
					fmt.Errorf("recoverd: %v, stack: %s", r, string(debug.Stack())),
					c.SendStatus(http.StatusInternalServerError),
				)
			}
		}()
		return c.Next()
	})
	s.r.Use(cors.New())
	s.base = s.r.Group("")
	if health.Name == "" || from_base {
		s.api = s.base.Group("/")
	} else {
		s.api = s.base.Group("/" + health.Name)
	}
	//sbragi.Fatal("formats", "json", string(hrj), "time", string(bv), "hrni", hrni, "hrne", hrne)
	s.api.Get("/health", func(c *fiber.Ctx) error {
		return h.WriteHealthReport(c)
		//return c.JSON(hr)
		//return c.JSON(h.GetHealthReport())
	})
	user := os.Getenv("debug.user")
	pass := os.Getenv("debug.pass")
	if user != "" && pass != "" && health.Name != "" {
		debug := s.api.Group("/debug")
		debug.Use(basicauth.New(basicauth.Config{
			Users: map[string]string{user: pass},
		}))
		debug.Get("/pprof/*type", func(c *fiber.Ctx) error {
			fmt.Println(c.Params("type"))
			switch c.Params("type") {
			case "/profile":
				return adaptor.HTTPHandlerFunc(pprof.Profile)(c)
				//pprof.Profile(c.Writer, c.Request)
			case "/trace":
				return adaptor.HTTPHandlerFunc(pprof.Trace)(c)
				//pprof.Trace(c.Writer, c.Request)
			case "/symbol":
				return adaptor.HTTPHandlerFunc(pprof.Symbol)(c)
				//pprof.Symbol(c.Writer, c.Request)
			default:
				return adaptor.HTTPHandlerFunc(pprof.Index)(c)
				//pprof.Index(c.Writer, c.Request)
			}
		})
	}
	return &s, nil
}

func (s *server) Base() fiber.Router {
	return s.base
}
func (s *server) API() fiber.Router {
	return s.api
}

func (s *server) Run() {
	err := s.r.Listen(fmt.Sprintf(":%d", s.Port()))
	if err != nil {
		sbragi.WithError(err).Fatal("while starting or running webserver")
	}
}

func (s *server) Port() uint16 {
	return s.port
}

func (s *server) Url() (u *url.URL) {
	u = &url.URL{}
	u.Scheme = "http"
	u.Host = fmt.Sprintf("%s:%d", health.GetOutboundIP(), s.Port())
	u.Path = "" //strings.TrimSuffix(s.base.BasePath(), "/")
	return
}

func UnmarshalBody[bodyT any](c *fiber.Ctx) (v bodyT, err error) {
	err = c.BodyParser(&v)
	var unmarshalErr *stdJson.UnmarshalTypeError
	if errors.As(err, &unmarshalErr) {
		err = fmt.Errorf(
			"wrong type provided for \"%s\" should be of type (%s) but got value {%s} after reading %d",
			unmarshalErr.Field,
			unmarshalErr.Type,
			unmarshalErr.Value,
			unmarshalErr.Offset,
		)
	}
	return
}

func ErrorResponse(c *fiber.Ctx, message string, httpStatusCode int) error {
	resp := make(map[string]string)
	resp["error"] = message
	return c.Status(httpStatusCode).JSON(resp)
}

func GetAuthHeader(c *fiber.Ctx) (header string) {
	return c.Get(AUTHORIZATION)
}

var ErrIncorrectContentType = fmt.Errorf(
	"http header did not contain key %s with value %s",
	CONTENT_TYPE,
	CONTENT_TYPE_JSON,
)
