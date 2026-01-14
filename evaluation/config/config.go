package config

import (
	"fmt"
	"log"
	"os"

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
		Host string
		Port string
		Name string
		User string
		Pass string
	}
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
	e.Timescale.Port = mustEnv("TS_PORT")
	e.Timescale.Name = mustEnv("TS_NAME")
	e.Timescale.User = mustEnv("TS_USER")
	e.Timescale.Pass = mustEnv("TS_PASS")

	return e
}

func (e Env) PostgresConn() string {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		e.Timescale.User,
		e.Timescale.Pass,
		e.Timescale.Name,
		e.Timescale.Host,
		e.Timescale.Port,
	)
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("Environment variable %s is not defined", key)
	}
	return v
}
