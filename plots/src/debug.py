import asyncio
import pydgraph
import os
import asyncpg
from dotenv import load_dotenv

# Cargar configuración manual para asegurar que no depende de clases externas
load_dotenv()

DGRAPH_URI = os.getenv("DGRAPH_URI", "localhost:9080")
PG_HOST = os.getenv("DB_HOST", "localhost")
PG_PORT = os.getenv("DB_PORT", "5432")
PG_USER = os.getenv("DB_USER", "postgres")
PG_PASS = os.getenv("DB_PASS", "password")
PG_NAME = os.getenv("DB_NAME", "dt_benchmark") # A veces da error si la BD no existe

async def run_diagnostics():
    print("="*60)
    print(" DIAGNOSTICO DE CONEXION Y ESCRITURA")
    print("="*60)

    # ---------------------------------------------------------
    # 1. PRUEBA DE DGRAPH (CENTRALIZED)
    # ---------------------------------------------------------
    print("\n[1] Probando Dgraph (gRPC)...")
    client_stub = None
    try:
        # Conexión
        print(f"   -> Conectando a {DGRAPH_URI}...", end=" ")
        client_stub = pydgraph.DgraphClientStub(DGRAPH_URI)
        client = pydgraph.DgraphClient(client_stub)
        # Probamos una operación simple para verificar conexión
        client.check_version() 
        print("OK")

        # Esquema
        print("   -> Aplicando Esquema...", end=" ")
        schema = """
        device_id: string @index(hash) .
        value: float .
        timestamp: int .
        type Device {
            device_id
            value
            timestamp
        }
        """
        op = pydgraph.Operation(schema=schema)
        client.alter(op)
        print("OK")

        # Escritura
        print("   -> Intentando Escritura (Mutation)...", end=" ")
        txn = client.txn()
        try:
            nquads = """
            _:x <device_id> "debug_test" .
            _:x <value> "99.9" .
            _:x <timestamp> "123456789" .
            _:x <dgraph.type> "Device" .
            """
            mutation = pydgraph.Mutation(set_nquads=nquads.encode('utf-8'))
            txn.mutate(mutation=mutation, commit_now=True)
            print("OK (Escritura exitosa)")
        finally:
            txn.discard()

    except Exception as e:
        print(f"\n   !!! ERROR CRÍTICO EN DGRAPH !!!")
        print(f"   Mensaje: {e}")
        print("   Consejo: Verifica que el contenedor corre en el puerto 9080 (gRPC), no el 8080 (HTTP).")
    finally:
        if client_stub: client_stub.close()

    # ---------------------------------------------------------
    # 2. PRUEBA DE TIMESCALE (DECENTRALIZED)
    # ---------------------------------------------------------
    print("\n[2] Probando TimescaleDB (PostgreSQL)...")
    try:
        print(f"   -> Conectando a {PG_HOST}:{PG_PORT} user={PG_USER} db={PG_NAME}...", end=" ")
        # Intentamos conectar a la BD específica
        try:
            conn = await asyncpg.connect(user=PG_USER, password=PG_PASS, database=PG_NAME, host=PG_HOST, port=PG_PORT)
        except asyncpg.InvalidCatalogNameError:
            print(f"\n   [Aviso] La base de datos '{PG_NAME}' no existe. Intentando conectar a 'postgres'...")
            conn = await asyncpg.connect(user=PG_USER, password=PG_PASS, database='postgres', host=PG_HOST, port=PG_PORT)
            # Crear la BD si falta
            await conn.execute(f'CREATE DATABASE {PG_NAME}')
            await conn.close()
            # Reconectar a la correcta
            conn = await asyncpg.connect(user=PG_USER, password=PG_PASS, database=PG_NAME, host=PG_HOST, port=PG_PORT)
        
        print("OK")

        print("   -> Creando Tabla de prueba...", end=" ")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS debug_data (
                time TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                value DOUBLE PRECISION NULL
            );
        """)
        print("OK")

        print("   -> Intentando Insert...", end=" ")
        await conn.execute("INSERT INTO debug_data (time, device_id, value) VALUES (NOW(), 'debug_dev', 123.45)")
        print("OK (Escritura exitosa)")
        
        await conn.close()

    except Exception as e:
        print(f"\n   !!! ERROR CRÍTICO EN TIMESCALE !!!")
        print(f"   Mensaje: {e}")
        print("   Consejo: Verifica usuario/pass en .env y que el puerto 5432 esté expuesto.")

if __name__ == "__main__":
    try:
        import sys
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except: pass
    asyncio.run(run_diagnostics())