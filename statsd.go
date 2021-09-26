package statsd

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
	"regexp"
)

var whitespace, _ = regexp.Compile("[\\s]+")

type statsD struct {
	logger *log.Entry

	addr     *net.UDPAddr
	failures int

	conn net.Conn
}

func newStatsD(host string, port int) (*statsD, error) {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, err
	}

	return &statsD{
		logger:   log.WithField("logger", "StatsD"),
		addr:     addr,
		failures: 0,
	}, nil
}

func (s *statsD) connect() error {
	if s.conn != nil {
		return fmt.Errorf("already connected")
	}

	conn, err := net.DialUDP("udp", nil, s.addr)
	if err != nil {
		return err
	}

	s.conn = conn

	return nil
}

func (s *statsD) send(name string, value string) {
	formatted := fmt.Sprintf("%s:%s|g", s.sanitize(name), s.sanitize(value))
	_, err := s.conn.Write([]byte(formatted))
	if err != nil {
		s.failures++

		if s.failures == 1 {
			s.logger.Warnf(
				"unable to send packet to statsd at '%s'",
				s.addr.String(),
			)
		} else {
			s.logger.Debugf(
				"unable to send packet to statsd at '%s'",
				s.addr.String(),
			)
		}
	} else {
		s.failures = 0
	}
}

func (s *statsD) close() error {
	if s.conn != nil {
		err := s.conn.Close()
		if err != nil {
			s.conn = nil
			return err
		}
	}

	s.conn = nil

	return nil
}

func (s *statsD) sanitize(str string) string {
	return whitespace.ReplaceAllString(str, "-")
}
