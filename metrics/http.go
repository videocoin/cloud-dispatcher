package metrics

import (
	"github.com/labstack/echo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HTTPServerConfig struct {
	Addr string
}

type HTTPServer struct {
	addr string
	e    *echo.Echo
}

func NewHTTPServer(addr string) (*HTTPServer, error) {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.DisableHTTP2 = true

	return &HTTPServer{
		addr: addr,
		e:    e,
	}, nil
}

func (s *HTTPServer) Start() error {
	s.routes()
	return s.e.Start(s.addr)
}

func (s *HTTPServer) Stop() error {
	return nil
}

func (s *HTTPServer) routes() {
	s.e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
}
