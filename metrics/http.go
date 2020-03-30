package metrics

import (
	"github.com/labstack/echo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

type HTTPServerConfig struct {
	Addr string
}

type HTTPServer struct {
	logger *logrus.Entry
	addr   string
	e      *echo.Echo
}

func NewHTTPServer(addr string, logger *logrus.Entry) (*HTTPServer, error) {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.DisableHTTP2 = true

	return &HTTPServer{
		logger: logger,
		addr:   addr,
		e:      e,
	}, nil
}

func (s *HTTPServer) Start() error {
	s.logger.Infof("http server listening on %s", s.addr)
	s.routes()
	return s.e.Start(s.addr)
}

func (s *HTTPServer) Stop() error {
	return nil
}

func (s *HTTPServer) routes() {
	s.e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
}
