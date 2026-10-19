package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Env struct {
	MQTT struct {
		Broker string
		Port   string
		User   string
		Pass   string
	}

	DgraphURI string

	Timescale struct {
		Host     string
		Ports    []string
		Name     string
		User     string
		Pass     string
		MaxISUs  int
		BasePort int
	}

	PrometheusURI       string
	KubernetesNamespace string
}

func LoadEnv() Env {
	if err := godotenv.Load(); err != nil {
		log.Println(".env not found")
	}

	var e Env

	e.MQTT.Broker = mustEnv("MQTT_BROKER")
	e.MQTT.Port = mustEnv("MQTT_PORT")
	e.MQTT.User = mustEnv("MQTT_USER")
	e.MQTT.Pass = mustEnv("MQTT_PASS")

	e.DgraphURI = mustEnv("DGRAPH_URI")

	e.Timescale.Host = mustEnv("TS_HOST")
	e.Timescale.Name = mustEnv("TS_NAME")
	e.Timescale.User = mustEnv("TS_USER")
	e.Timescale.Pass = mustEnv("TS_PASS")

	maxISUs := 8
	if v := os.Getenv("TS_NUM_ISUS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			maxISUs = parsed
		}
	}
	e.Timescale.MaxISUs = maxISUs

	// Support explicit comma-separated TS_PORTS or consecutive ports starting from TS_BASE_PORT (or TS_PORT)
	if portsEnv := os.Getenv("TS_PORTS"); portsEnv != "" {
		parts := strings.Split(portsEnv, ",")
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				e.Timescale.Ports = append(e.Timescale.Ports, trimmed)
			}
		}
	} else {
		basePortStr := os.Getenv("TS_BASE_PORT")
		if basePortStr == "" {
			basePortStr = os.Getenv("TS_PORT")
		}
		if basePortStr == "" {
			basePortStr = "45521"
		}
		basePort, err := strconv.Atoi(basePortStr)
		if err != nil {
			log.Fatalf("Invalid TS_BASE_PORT / TS_PORT: %v", err)
		}
		e.Timescale.BasePort = basePort
		for i := 0; i < maxISUs; i++ {
			e.Timescale.Ports = append(e.Timescale.Ports, strconv.Itoa(basePort+i))
		}
	}

	e.PrometheusURI = mustEnv("PROMETHEUS_URL")
	e.KubernetesNamespace = mustEnv("KUBERNETES_NAMESPACE")

	return e
}

// PostgresConn returns connection string for ISU 0 (backward-compatible)
func (e Env) PostgresConn() string {
	return e.PostgresConnForISU(0)
}

// PostgresConnForISU returns connection string for a specific ISU index (0 to MaxISUs-1)
func (e Env) PostgresConnForISU(isuIndex int) string {
	if len(e.Timescale.Ports) == 0 {
		log.Fatalf("No TimescaleDB ports configured")
	}
	port := e.Timescale.Ports[isuIndex%len(e.Timescale.Ports)]
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		e.Timescale.User,
		e.Timescale.Pass,
		e.Timescale.Name,
		e.Timescale.Host,
		port,
	)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("Environment variable %s is not defined", key)
	}
	return v
}
