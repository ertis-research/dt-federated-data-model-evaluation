import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Databases
    DGRAPH_URI = os.getenv("DGRAPH_URI", "localhost:9080")
    
    PG_HOST = os.getenv("DB_HOST", "localhost")
    PG_PORT = os.getenv("DB_PORT", "5432")
    PG_NAME = os.getenv("DB_NAME", "dt_benchmark")
    PG_USER = os.getenv("DB_USER", "postgres")
    PG_PASS = os.getenv("DB_PASS", "password")
    
    # Tuning: Critical for high concurrency ingestion in Decentralized Arch
    PG_POOL_MIN = 10
    PG_POOL_MAX = 80

    # MQTT
    MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
    MQTT_USER = os.getenv("MQTT_USER", None)
    MQTT_PASS = os.getenv("MQTT_PASS", None)