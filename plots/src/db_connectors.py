import pydgraph
import asyncpg
from config import Config

class DGraphManager:
    def __init__(self):
        self.client_stub = None
        self.client = None

    def connect(self):
        self.client_stub = pydgraph.DgraphClientStub(Config.DGRAPH_URI)
        self.client = pydgraph.DgraphClient(self.client_stub)

    def close(self):
        if self.client_stub: self.client_stub.close()

    def drop_all(self):
        """Wipes the entire database to start fresh for a new scenario."""
        op = pydgraph.Operation(drop_all=True)
        self.client.alter(op)

    def setup_schema(self):
        """
        Defines the complete schema for the Topology Generator.
        Fixed: Added missing predicates 'name', 'level', 'asset_id' and relationships.
        """
        schema = """
        # --- Predicates (Properties) ---
        device_id: string @index(hash) .
        type: string @index(exact) .
        name: string @index(term) .
        location: string .
        value: float .
        timestamp: int .
        level: string .
        asset_id: string @index(hash) .

        # --- Relationships (Edges) ---
        has_floor: [uid] .
        has_room: [uid] .
        contains_device: [uid] .

        # --- Types Definitions ---
        type Device {
            device_id
            type
            location
            value
            timestamp
        }
        
        type Building {
            type
            name
            has_floor
        }
        
        type Floor {
            type
            level
            has_room
        }

        type Room {
            type
            name
            contains_device
        }

        type Asset {
            type
            name
            asset_id
        }
        """
        op = pydgraph.Operation(schema=schema)
        self.client.alter(op)

class TimescaleManager:
    def __init__(self):
        self.pool = None

    async def connect(self):
        # Increased pool size to prevent 'connection pool exhausted' during massive bursts
        self.pool = await asyncpg.create_pool(
            user=Config.PG_USER, password=Config.PG_PASS,
            database=Config.PG_NAME, host=Config.PG_HOST, port=Config.PG_PORT,
            min_size=Config.PG_POOL_MIN, 
            max_size=Config.PG_POOL_MAX
        )

    async def close(self):
        if self.pool: await self.pool.close()

    async def clean_table(self):
        """Truncates table for clean slate."""
        async with self.pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE sensor_data;")

    async def setup_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    time TIMESTAMPTZ NOT NULL,
                    device_id TEXT NOT NULL,
                    value DOUBLE PRECISION NULL
                );
            """)
            try:
                await conn.execute("SELECT create_hypertable('sensor_data', 'time', if_not_exists => TRUE);")
            except: pass
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_device_time ON sensor_data (device_id, time DESC);")