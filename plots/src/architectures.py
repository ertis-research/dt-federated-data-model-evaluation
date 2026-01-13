import json
import pydgraph
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

executor = ThreadPoolExecutor(max_workers=20)

class CentralizedArchitecture:
    def __init__(self, dgraph_client):
        self.dgraph = dgraph_client

    def _ingest_sync(self, device_id, value, timestamp):
        retries = 3
        start_db = time.perf_counter() # <--- Inicio cronómetro DB
        
        while retries > 0:
            txn = self.dgraph.txn()
            try:
                query = """{ u as var(func: eq(device_id, "%s")) }""" % device_id
                nquads = """
                uid(u) <device_id> "%s" .
                uid(u) <value> "%f" .
                uid(u) <timestamp> "%d" .
                uid(u) <dgraph.type> "Device" .
                """ % (device_id, value, timestamp)
                
                mutation = pydgraph.Mutation(set_nquads=nquads.encode('utf-8'))
                req = pydgraph.Request(query=query, mutations=[mutation], commit_now=True)
                txn.do_request(req)
                
                # Éxito: Calculamos tiempo
                db_duration = (time.perf_counter() - start_db) * 1000
                return True, db_duration
                
            except pydgraph.AbortedError:
                retries -= 1
            except Exception as e:
                print(f"[ERROR DGRAPH] {e}") # <--- AÑADIR ESTO
                break
        
        return False, 0.0

    async def ingest_data(self, device_id, value, timestamp):
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(executor, self._ingest_sync, device_id, value, timestamp)
        except Exception:
            return False, 0.0

    # ... (query_state se mantiene igual) ...
    async def query_state(self, device_id):
        def _query():
            query = """{
                q(func: eq(device_id, "%s")) {
                    device_id, value, timestamp
                }
            }""" % device_id
            txn = self.dgraph.txn(read_only=True)
            try:
                res = txn.query(query)
                return json.loads(res.json)
            finally:
                txn.discard()
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(executor, _query)


class DecentralizedArchitecture:
    def __init__(self, dgraph_client, timescale_pool):
        self.dgraph = dgraph_client
        self.ts_pool = timescale_pool

    async def ingest_data(self, device_id, value, timestamp):
        start_db = time.perf_counter() # <--- Inicio cronómetro DB
        try:
            async with self.ts_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO sensor_data (time, device_id, value) VALUES (NOW(), $1, $2)",
                    device_id, value
                )
            
            db_duration = (time.perf_counter() - start_db) * 1000
            return True, db_duration
            
        except Exception as e:
            print(f"[ERROR TIMESCALE] {e}") # <--- AÑADIR ESTO
            return False, 0.0

    # ... (query_state_federated se mantiene igual) ...
    async def query_state_federated(self, device_id):
        loop = asyncio.get_running_loop()
        def _get_meta():
            txn = self.dgraph.txn(read_only=True)
            try:
                q = '{ q(func: eq(device_id, "%s")) { device_id } }' % device_id
                res = txn.query(q)
                return json.loads(res.json)
            finally:
                txn.discard()
        try:
            kg_data = await loop.run_in_executor(executor, _get_meta)
            if not kg_data.get('q'): return None
            async with self.ts_pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT value, time FROM sensor_data WHERE device_id = $1 ORDER BY time DESC LIMIT 1",
                    device_id
                )
            return {"metadata": kg_data, "dynamic": dict(row) if row else None}
        except Exception:
            return None