package statsd

import (
	"context"
	"fmt"
	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var percentiles = []float64{0.5, 0.75, 0.95, 0.98, 0.99, 0.999}

type StatsDReporterOption func(reporter *StatsDReporter)

func PrefixedWith(prefix string) StatsDReporterOption {
	return func(reporter *StatsDReporter) {
		reporter.prefix = prefix
	}
}

func ConvertDurationsTo(durationUnit time.Duration) StatsDReporterOption {
	return func(reporter *StatsDReporter) {
		reporter.durationUnit = durationUnit
	}
}

type StatsDReporter struct {
	logger *log.Entry

	statsD       *statsD
	registry     metrics.Registry // Registry to be exported
	durationUnit time.Duration    // Time conversion unit for durations
	prefix       string           // Prefix to be prepended to metric names

	cancelFunc context.CancelFunc
}

func NewStatsDReporter(
	registry metrics.Registry,
	host string,
	port int,
	options ...StatsDReporterOption,
) (*StatsDReporter, error) {
	statsD, err := newStatsD(host, port)
	if err != nil {
		return nil, err
	}

	reporter := &StatsDReporter{
		logger:       log.WithField("logger", "StatsDReporter"),
		statsD:       statsD,
		registry:     registry,
		durationUnit: time.Millisecond,
		prefix:       "",
	}

	for _, option := range options {
		option(reporter)
	}

	return reporter, nil
}

func (s *StatsDReporter) Start(period int64, unit time.Duration) {
	s.StartWithDelay(period, period, unit)
}

func (s *StatsDReporter) StartWithDelay(initialDelay, period int64, unit time.Duration) {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	s.cancelFunc = cancelFunc

	go func() {
		time.Sleep(unit * time.Duration(initialDelay))

		ticker := time.Tick(unit * time.Duration(period))

		for {
			select {
			case <-ticker:
				s.Report()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (s *StatsDReporter) Stop() {
	s.Report()

	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

func (s *StatsDReporter) Report() {
	err := s.statsD.connect()
	if err != nil {
		s.logger.Warn("Unable to report to StatsD: ", err)
		return
	}

	// for each metric in the registry format into statsd format and send
	s.registry.Each(func(name string, metric interface{}) {
		switch m := metric.(type) {
		case metrics.Counter:
			s.reportCounter(name, m)
		case metrics.Gauge:
			s.reportGauge(name, m)
		case metrics.GaugeFloat64:
			s.reportGaugeFloat(name, m)
		case metrics.Histogram:
			s.reportHistogram(name, m)
		case metrics.Meter:
			s.reportMeter(name, m)
		case metrics.Timer:
			s.reportTimer(name, m)
		default:
			s.logger.Debugf("[WARN] No Metric: %s - %s - %v", s.prefix, name, reflect.TypeOf(m))
		}
	})

	err = s.statsD.close()
	if err != nil {
		s.logger.Debug("Error disconnecting from StatsD: ", err)
	}
}

func (s *StatsDReporter) reportTimer(name string, timer metrics.Timer) {
	snapshot := timer.Snapshot()
	ps := snapshot.Percentiles(percentiles)
	s.statsD.send(s.prefixed(name, "samples"), s.formatLong(snapshot.Count()))
	s.statsD.send(s.prefixed(name, "max"), s.formatLong(s.convertDuration(snapshot.Max())))
	s.statsD.send(s.prefixed(name, "mean"), s.formatFloat(s.convertDurationFloat(snapshot.Mean())))
	s.statsD.send(s.prefixed(name, "min"), s.formatLong(s.convertDuration(snapshot.Min())))
	s.statsD.send(s.prefixed(name, "stddev"), s.formatFloat(s.convertDurationFloat(snapshot.StdDev())))

	for psIdx, psKey := range percentiles {
		key := "p" + strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)

		s.statsD.send(s.prefixed(name, key), s.formatFloat(s.convertDurationFloat(ps[psIdx])))
	}

	s.statsD.send(s.prefixed(name, "m1_rate"), s.formatFloat(snapshot.Rate1()))
	s.statsD.send(s.prefixed(name, "m5_rate"), s.formatFloat(snapshot.Rate5()))
	s.statsD.send(s.prefixed(name, "m15_rate"), s.formatFloat(snapshot.Rate15()))
	s.statsD.send(s.prefixed(name, "mean_rate"), s.formatFloat(snapshot.RateMean()))
}

func (s *StatsDReporter) reportMeter(name string, meter metrics.Meter) {
	snapshot := meter.Snapshot()

	s.statsD.send(s.prefixed(name, "samples"), s.formatLong(snapshot.Count()))
	s.statsD.send(s.prefixed(name, "m1_rate"), s.formatFloat(snapshot.Rate1()))
	s.statsD.send(s.prefixed(name, "m5_rate"), s.formatFloat(snapshot.Rate5()))
	s.statsD.send(s.prefixed(name, "m15_rate"), s.formatFloat(snapshot.Rate15()))
	s.statsD.send(s.prefixed(name, "mean_rate"), s.formatFloat(snapshot.RateMean()))
}

func (s *StatsDReporter) reportHistogram(name string, histogram metrics.Histogram) {
	snapshot := histogram.Snapshot()
	ps := snapshot.Percentiles(percentiles)
	s.statsD.send(s.prefixed(name, "samples"), s.formatLong(histogram.Count()))
	s.statsD.send(s.prefixed(name, "max"), s.formatLong(snapshot.Max()))
	s.statsD.send(s.prefixed(name, "mean"), s.formatFloat(snapshot.Mean()))
	s.statsD.send(s.prefixed(name, "min"), s.formatLong(snapshot.Min()))
	s.statsD.send(s.prefixed(name, "stddev"), s.formatFloat(snapshot.StdDev()))

	for psIdx, psKey := range percentiles {
		key := "p" + strings.Replace(strconv.FormatFloat(psKey*100.0, 'f', -1, 64), ".", "", 1)

		s.statsD.send(s.prefixed(name, key), s.formatFloat(ps[psIdx]))
	}
}

func (s *StatsDReporter) reportCounter(name string, counter metrics.Counter) {
	s.statsD.send(s.prefixed(name), s.formatLong(counter.Count()))
}

func (s *StatsDReporter) reportGauge(name string, gauge metrics.Gauge) {
	s.statsD.send(s.prefixed(name), s.formatLong(gauge.Value()))
}

func (s *StatsDReporter) reportGaugeFloat(name string, gauge metrics.GaugeFloat64) {
	s.statsD.send(s.prefixed(name), s.formatFloat(gauge.Value()))
}

func (s *StatsDReporter) prefixed(components ...string) string {
	allComponents := strings.Join(components, ".")

	if len(s.prefix) > 0 {
		return fmt.Sprintf("%s.%s", s.prefix, allComponents)
	}

	return allComponents
}

func (s *StatsDReporter) formatLong(val int64) string {
	return fmt.Sprintf("%d", val)
}

func (s *StatsDReporter) formatFloat(val float64) string {
	return fmt.Sprintf("%2.2f", val)
}

func (s *StatsDReporter) convertDuration(duration int64) int64 {
	return duration / int64(s.durationUnit)
}

func (s *StatsDReporter) convertDurationFloat(duration float64) float64 {
	return duration / float64(s.durationUnit)
}
