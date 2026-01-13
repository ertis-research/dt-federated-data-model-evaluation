import asyncio
import json
import time
from aiomqtt import Client
from config import Config

class MqttIngestionService:
    def __init__(self, architecture, topic_prefix):
        self.arch = architecture
        self.topic_filter = f"{topic_prefix}/#"
        
        # MÉTRICAS
        self.e2e_latencies = [] # Latencia Total (Red + Python + BD)
        self.db_latencies = []  # Latencia Pura BD (Lo importante)
        
        self.msgs_seen = 0      # Mensajes que llegaron al código Python
        self.msgs_saved = 0     # Mensajes confirmados por la BD
        
        # Set de IDs para evitar duplicados y verificar integridad
        self.saved_ids = set()  # <--- SE LLAMA ASÍ AHORA
        
        self.running = False
        self.active_run_id = None 

    def prepare_run(self, run_id):
        self.active_run_id = run_id
        self.e2e_latencies = []
        self.db_latencies = []
        self.msgs_seen = 0
        self.msgs_saved = 0
        self.saved_ids = set()

    async def start_consumer(self):
        self.running = True
        while self.running:
            try:
                async with Client(hostname=Config.MQTT_BROKER, port=Config.MQTT_PORT) as client:
                    await client.subscribe(self.topic_filter)
                    async for message in client.messages:
                        if not self.running: break
                        asyncio.create_task(self._process_message(message))
            except Exception:
                await asyncio.sleep(1)

    async def _process_message(self, message):
        try:
            payload = json.loads(message.payload.decode())
            # Filtramos mensajes de ejecuciones anteriores
            if payload.get('run_id') != self.active_run_id: return

            self.msgs_seen += 1
            recv_time = time.time()
            
            # Intentamos guardar (retorna latencia pura si ok)
            success, db_time_ms = await self.arch.ingest_data(
                payload['device_id'], 
                payload['value'], 
                payload['timestamp']
            )
            
            if success:
                self.msgs_saved += 1
                send_time = payload['send_time']
                
                # Registramos ambas latencias
                self.e2e_latencies.append((recv_time - send_time) * 1000)
                self.db_latencies.append(db_time_ms)
                
                # Guardamos ID para control
                self.saved_ids.add(payload['msg_id'])

        except Exception:
            pass 
    
    def stop(self): self.running = False