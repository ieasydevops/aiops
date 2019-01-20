package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	influx "github.com/influxdata/influxdb/client/v2"
	"github.com/meixinyun/common/pkg/util/sets"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/documentation/examples/remote_storage/remote_storage_adapter/influxdb"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	MLabelName = []string{"I", "E", "C", "P", "L"}

	LI                = "I"
	LE                = "E"
	LC                = "C"
	LP                = "P"
	LL                = "L"
	AiChallengeMetric = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "aiops",
		Subsystem: "ingester",
		Help:      "aiops challenge example metric data",
		Name:      "ai_metric",
	}, []string{LI, LE, LC, LP, LL})
)

type config struct {
	dataBase                string
	ingestionFile           string
	influxdbURL             string
	influxdbRetentionPolicy string
	influxdbUsername        string
	influxdbDatabase        string
	influxdbPassword        string
	remoteTimeout           time.Duration
	listenAddr              string
	telemetryPath           string
	logLevel                string
}

func parseFlags() *config {
	cfg := &config{
		influxdbPassword: os.Getenv("INFLUXDB_PW"),
	}

	flag.StringVar(&cfg.ingestionFile, "ingestionFile", "../../data/2019AIOps_data/1535731200000.csv",
		"load csv data.",
	)

	flag.StringVar(&cfg.dataBase, "dataBase", "../../data/2019AIOps_data",
		"load csv data.",
	)

	flag.StringVar(&cfg.influxdbURL, "influxdb-url", "http://127.0.0.1:8086",
		"The URL of the remote InfluxDB server to send samples to. None, if empty.",
	)
	flag.StringVar(&cfg.influxdbRetentionPolicy, "influxdb.retention-policy", "",
		"The InfluxDB retention policy to use.",
	)
	flag.StringVar(&cfg.influxdbUsername, "influxdb.username", "admin",
		"The username to use when sending samples to InfluxDB. The corresponding password must be provided via the INFLUXDB_PW environment variable.",
	)

	flag.StringVar(&cfg.influxdbPassword, "influxdb.passwd", "admin",
		"The password to use when sending samples to InfluxD",
	)
	flag.StringVar(&cfg.influxdbDatabase, "influxdb.database", "prometheus",
		"The name of the database to use for storing samples in InfluxDB.",
	)
	flag.DurationVar(&cfg.remoteTimeout, "send-timeout", 30*time.Second,
		"The timeout to use when sending samples to the remote storage.",
	)
	flag.StringVar(&cfg.listenAddr, "web.listen-address", ":9201", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.logLevel, "log.level", "debug", "Only log messages with the given severity or above. One of: [debug, info, warn, error]")
	flag.Parse()

	return cfg
}

func main() {
	cfg := parseFlags()
	logLevel := promlog.AllowedLevel{}
	logLevel.Set(cfg.logLevel)
	logger := promlog.New(logLevel)
	fileNames := FileDir(logger, cfg)
	writers := buildClients(logger, cfg)
	var wg sync.WaitGroup
	for _, fName := range fileNames {
		samples := ingestionFromFile(logger, cfg, fName)
		for _, w := range writers {
			wg.Add(1)
			go func(rw writer) {
				sendSamples(logger, rw, samples)
				wg.Done()
			}(w)
		}
	}
	wg.Wait()
}

type writer interface {
	Write(samples model.Samples) error
	Name() string
}

func buildClients(logger log.Logger, cfg *config) []writer {
	var writers []writer
	if cfg.influxdbURL != "" {
		url, err := url.Parse(cfg.influxdbURL)
		if err != nil {
			level.Error(logger).Log("msg", "Failed to parse InfluxDB URL", "url", cfg.influxdbURL, "err", err)
			os.Exit(1)
		}
		conf := influx.HTTPConfig{
			Addr:     url.String(),
			Username: cfg.influxdbUsername,
			Password: cfg.influxdbPassword,
			Timeout:  cfg.remoteTimeout,
		}

		level.Debug(logger).Log("msg", "influxdbCfg", conf.Addr)

		c := influxdb.NewClient(
			log.With(logger, "storage", "InfluxDB"),
			conf,
			cfg.influxdbDatabase,
			cfg.influxdbRetentionPolicy,
		)
		prometheus.MustRegister(c)
		writers = append(writers, c)

	}
	level.Info(logger).Log("msg", "Starting up...")
	return writers
}

func FileDir(log log.Logger, cfg *config) []string {
	ret := sets.NewString()
	files, _ := ioutil.ReadDir(cfg.dataBase)
	for _, f := range files {
		ret.Insert(strings.TrimRight(f.Name(), ".csv"))
	}
	return ret.List()
}

func ingestionFromFile(logger log.Logger, cfg *config, fileName string) model.Samples {

	level.Info(logger).Log("msg", "ingestionFile ", "path", strings.Join([]string{cfg.dataBase, fileName}, "/"))

	file, err := os.Open(cfg.ingestionFile)
	if err != nil {
		fmt.Println("Error:", err)
		level.Info(logger).Log("msg", "ingestionFile", err)
		return nil
	}

	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comment = '#' //可以设置读入文件中的注释符
	reader.Comma = ','   //默认是逗号，也可以自己设置
	var samples model.Samples
	k := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
			return nil
		}

		if len(record) == 6 {
			if tm, err := strconv.ParseInt(fileName, 10, 64); err == nil {
				if mv, err := strconv.ParseFloat(record[5], 64); err == nil {
					level.Debug(logger).Log("msg", "build data samples", "timestamp", tm)
					samples = append(samples, &model.Sample{
						Metric: model.Metric(model.LabelSet{
							model.LabelName(model.MetricNameLabel): model.LabelValue("ops_metric"),
							model.LabelName(MLabelName[0]):         model.LabelValue(record[0]),
							model.LabelName(MLabelName[1]):         model.LabelValue(record[1]),
							model.LabelName(MLabelName[2]):         model.LabelValue(record[2]),
							model.LabelName(MLabelName[3]):         model.LabelValue(record[3]),
							model.LabelName(MLabelName[4]):         model.LabelValue(record[4]),
						}),
						Value:     model.SampleValue(mv),
						Timestamp: model.TimeFromUnix(tm / 1000),
					})
				} else {
					level.Info(logger).Log("msg", "parserMetricValueError", err)
				}
			} else {
				level.Info(logger).Log("msg", "parserFileNameError", err)
			}
		}
		k = k + 1
	}
	return samples
}

func sendSamples(logger log.Logger, w writer, samples model.Samples) {
	err := w.Write(samples)
	if err != nil {
		level.Warn(logger).Log("msg", "Error sending samples to remote storage", "err", err, "storage", w.Name(), "num_samples", len(samples))
	}
	level.Info(logger).Log("msg", "send samples to remote storage OK ", "num_samples", len(samples))
}
