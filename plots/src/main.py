import asyncio
import logging
import sys
import numpy as np
import pandas as pd
from tqdm import tqdm 
from db_connectors import DGraphManager, TimescaleManager
from architectures import CentralizedArchitecture, DecentralizedArchitecture
from benchmark import BenchmarkRunner
from mqtt_service import MqttIngestionService
from topology_generator import GraphTopologyGenerator
from plotter import generate_comparative_plots

# Configuración de Logs (Silenciar ruido)
logging.basicConfig(stream=sys.stderr, level=logging.CRITICAL)

async def main():
    print("\n" + "="*80)
    print("  IEEE BENCHMARK: DECOUPLING DYNAMIC STATE FROM SEMANTIC STRUCTURE")
    print("="*80)
    
    # Conexiones
    dg = DGraphManager(); dg.connect()
    ts = TimescaleManager(); await ts.connect()
    topo_gen = GraphTopologyGenerator(dg.client)

    centralized = CentralizedArchitecture(dg.client)
    decentralized = DecentralizedArchitecture(dg.client, ts.pool)
    
    mqtt_c = MqttIngestionService(centralized, "dt/centralized")
    mqtt_d = MqttIngestionService(decentralized, "dt/decentralized")
    
    t1 = asyncio.create_task(mqtt_c.start_consumer())
    t2 = asyncio.create_task(mqtt_d.start_consumer())

    runner = BenchmarkRunner()

    # --- ESCENARIOS ---
    scenarios = [
        {
            "name": "Home", 
            "sensors": 20, "noise_factor": 10, "rate": 20, "duration": 5
        },
        {
            "name": "Building", 
            "sensors": 100, "noise_factor": 50, "rate": 200, "duration": 5
        },
        {
            "name": "Factory", 
            "sensors": 500, "noise_factor": 20, "rate": 500, "duration": 5
        },
        {
            "name": "Smart City", 
            "sensors": 1000, "noise_factor": 20, "rate": 1000, "duration": 5
        }
    ]
    
    REPETITIONS = 5
    results_data = []

    for sc in scenarios:
        total_nodes = sc['sensors'] * sc['noise_factor']
        update_freq_hz = sc['rate'] / sc['sensors'] 

        print(f"\n" + "-"*80)
        print(f" SCENARIO: {sc['name']}")
        print(f"  - Graph Nodes:     {total_nodes} (Complexity)")
        print(f"  - Active Sensors:  {sc['sensors']}")
        print(f"  - Update Freq:     {update_freq_hz:.1f} Hz/sensor")
        print(f"  - Total Load:      {sc['rate']} msg/s")
        print("-"*80)
        
        # --- PREPARACIÓN DEL GRAFO ---
        print(" -> Generating Topology...", end=" ")
        
        # CORRECCIÓN AQUÍ:
        dg.drop_all()       # Síncrono (pydgraph)
        dg.setup_schema()   # Síncrono (pydgraph)
        await ts.clean_table() # ASÍNCRONO (asyncpg) -> Faltaba await
        
        topo_gen.generate_topology(sc['sensors'], sc['noise_factor'])
        print("Done.")

        # --- CENTRALIZED ---
        db_lats, e2e_lats, losses = [], [], []
        pbar = tqdm(total=REPETITIONS, desc="  Centralized  ", ncols=120, colour='red')
        for _ in range(REPETITIONS):
            res = await runner.run_rate_test('centralized', sc['rate'], sc['duration'], sc['sensors'], mqtt_c)
            db_lats.append(res['db_latency'])
            e2e_lats.append(res['e2e_latency'])
            losses.append(res['loss_pct'])
            
            pbar.set_postfix({'DB_Lat': f"{res['db_latency']:.1f}ms", 'Loss': f"{res['loss_pct']:.1f}%"})
            pbar.update(1)
            await asyncio.sleep(1)
        pbar.close()
        
        results_data.append({
            "Scenario": sc['name'], "Total_Nodes": total_nodes, "Update_Hz": update_freq_hz,
            "Architecture": "Centralized",
            "DB_Latency_ms": np.mean(db_lats), "E2E_Latency_ms": np.mean(e2e_lats), "Loss_pct": np.mean(losses)
        })

        # --- DECENTRALIZED ---
        # Limpiamos solo la tabla de series temporales, mantenemos el grafo de metadatos
        await ts.clean_table() 
        
        db_lats, e2e_lats, losses = [], [], []
        pbar = tqdm(total=REPETITIONS, desc="  Decentralized", ncols=120, colour='green')
        for _ in range(REPETITIONS):
            res = await runner.run_rate_test('decentralized', sc['rate'], sc['duration'], sc['sensors'], mqtt_d)
            db_lats.append(res['db_latency'])
            e2e_lats.append(res['e2e_latency'])
            losses.append(res['loss_pct'])
            
            pbar.set_postfix({'DB_Lat': f"{res['db_latency']:.1f}ms", 'Loss': f"{res['loss_pct']:.1f}%"})
            pbar.update(1)
            await asyncio.sleep(1)
        pbar.close()

        results_data.append({
            "Scenario": sc['name'], "Total_Nodes": total_nodes, "Update_Hz": update_freq_hz,
            "Architecture": "Decentralized",
            "DB_Latency_ms": np.mean(db_lats), "E2E_Latency_ms": np.mean(e2e_lats), "Loss_pct": np.mean(losses)
        })

    # Guardar y Graficar
    df = pd.DataFrame(results_data)
    df.to_csv("benchmark_results.csv", index=False)
    generate_comparative_plots(df)

    # Limpieza final
    mqtt_c.stop(); mqtt_d.stop()
    t1.cancel(); t2.cancel()
    dg.close(); await ts.close()
    
    print("\n[Evaluation Complete]")

if __name__ == "__main__":
    try:
        # Fix para Windows y asyncio
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except: pass
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except RuntimeError as e:
        # Ignorar error de cierre sucio
        if str(e) != "Event loop is closed":
            raise e