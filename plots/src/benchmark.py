import time
import asyncio
import json
import uuid
import random
import numpy as np
from aiomqtt import Client
from config import Config

class BenchmarkRunner:
    
    async def _publish_at_rate(self, topic_base, target_rate, duration_sec, n_sensors, run_id):
        successfully_sent_ids = set()
        target_delay = 1.0 / target_rate if target_rate > 0 else 0
        
        start_time = time.time()
        msg_count = 0
        
        async with Client(
            hostname=Config.MQTT_BROKER,
            port=Config.MQTT_PORT,
            username=Config.MQTT_USER,
            password=Config.MQTT_PASS
        ) as client:
            
            while (time.time() - start_time) < duration_sec:
                loop_start = time.perf_counter()
                
                msg_id = str(uuid.uuid4())
                sensor_id = f"sensor_{msg_count % n_sensors}"
                
                payload = {
                    "run_id": run_id,
                    "msg_id": msg_id,
                    "device_id": sensor_id,
                    "value": random.uniform(20.0, 30.0),
                    "timestamp": int(time.time()),
                    "send_time": time.time()
                }
                topic = f"{topic_base}/{sensor_id}"
                
                try:
                    await client.publish(topic, payload=json.dumps(payload), qos=0)
                    successfully_sent_ids.add(msg_id)
                except Exception:
                    pass
                
                msg_count += 1
                elapsed = time.perf_counter() - loop_start
                if target_delay - elapsed > 0:
                    await asyncio.sleep(target_delay - elapsed)

        real_duration = time.time() - start_time
        actual_rate = len(successfully_sent_ids) / real_duration if real_duration > 0 else 0
        
        return successfully_sent_ids, actual_rate

    async def run_rate_test(self, arch_name, target_rate, duration, n_sensors, consumer):
        run_id = str(uuid.uuid4())
        consumer.prepare_run(run_id)
        topic_base = f"dt/{arch_name}"
        
        # 1. Publicar
        sent_ids, actual_tx_rate = await self._publish_at_rate(
            topic_base, target_rate, duration, n_sensors, run_id
        )
        
        # 2. Esperar procesamiento (Drain)
        # Timeout base + extra según volumen
        timeout = 5.0 + (len(sent_ids) / 500.0) 
        start_wait = time.time()
        
        last_saved_count = 0
        stable_cycles = 0
        
        # Bucle de espera inteligente
        while True:
            # Condición de salida 1: Timeout absoluto
            if time.time() - start_wait > timeout:
                break
            
            # Condición de salida 2: Todo guardado (Caso ideal 0% loss)
            # CORREGIDO: Usamos saved_ids en lugar de received_ids
            if len(consumer.saved_ids) >= len(sent_ids):
                break
            
            # Condición de salida 3: Estabilidad (No llegan nuevos msgs en un rato)
            # Esto es vital para cuando hay Data Loss (nunca llegaremos al total enviado)
            current_saved = len(consumer.saved_ids)
            if current_saved == last_saved_count and current_saved > 0:
                stable_cycles += 1
                if stable_cycles > 15: # ~1.5 segundos sin cambios
                    break
            else:
                stable_cycles = 0
                
            last_saved_count = current_saved
            await asyncio.sleep(0.1)
            
        # 3. Métricas
        total_arrived = consumer.msgs_seen
        total_saved = consumer.msgs_saved
        
        # Cálculo de pérdida de BD
        if total_arrived > 0:
            db_loss_pct = ((total_arrived - total_saved) / total_arrived) * 100
        else:
            db_loss_pct = 0.0
            
        # Medias seguras (evitar NaN)
        e2e_lat = np.mean(consumer.e2e_latencies) if consumer.e2e_latencies else 0.0
        db_lat = np.mean(consumer.db_latencies) if consumer.db_latencies else 0.0
        
        return {
            "loss_pct": db_loss_pct,
            "e2e_latency": e2e_lat,
            "db_latency": db_lat,
            "actual_rate": actual_tx_rate,
            "sent": len(sent_ids),
            "seen_consumer": total_arrived,
            "saved": total_saved
        }

    async def execute_query_test(self, arch, n_queries):
        try: await arch.ingest_data("sensor_0", 25.0, int(time.time()))
        except: pass
        
        latencies = []
        for _ in range(n_queries):
            t0 = time.perf_counter()
            if hasattr(arch, 'query_state_federated'):
                await arch.query_state_federated("sensor_0")
            else:
                await arch.query_state("sensor_0")
            latencies.append((time.perf_counter() - t0) * 1000)
            
        return np.mean(latencies) if latencies else 0.0