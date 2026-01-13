import pydgraph
import random
import json

class GraphTopologyGenerator:
    """
    Generates a realistic Digital Twin topology (Context) to stress the Graph.
    Structure: Building -> Floor -> Room -> Sensor
    """
    def __init__(self, dgraph_client):
        self.client = dgraph_client

    def generate_topology(self, num_sensors, complexity_factor=1):
        """
        Creates a graph hierarchy.
        :param num_sensors: Exact number of active sensors for the test.
        :param complexity_factor: Multiplier for extra context nodes (assets, people, etc)
        """
        # Calculate structure based on sensors
        sensors_per_room = 2
        rooms_per_floor = 10
        
        num_rooms = max(1, num_sensors // sensors_per_room)
        num_floors = max(1, num_rooms // rooms_per_floor)
        num_buildings = 1
        
        # Extra context nodes (Assets unrelated to sensors but filling the index)
        num_assets = num_sensors * complexity_factor

        print(f"    [Graph Gen] Creating {num_buildings} Bldgs, {num_floors} Flrs, {num_rooms} Rooms, {num_sensors} Sensors, {num_assets} Assets...")

        txn = self.client.txn()
        try:
            nquads = []
            
            # 1. Create Buildings & Floors (Hierarchy)
            b_uid = "_:b1"
            nquads.append(f'{b_uid} <type> "Building" .')
            nquads.append(f'{b_uid} <name> "Main HQ" .')
            
            floor_uids = []
            for f in range(num_floors):
                f_uid = f"_:f{f}"
                floor_uids.append(f_uid)
                nquads.append(f'{f_uid} <type> "Floor" .')
                nquads.append(f'{f_uid} <level> "{f}" .')
                nquads.append(f'{b_uid} <has_floor> {f_uid} .')

            # 2. Create Rooms & Link to Floors
            room_uids = []
            for r in range(num_rooms):
                r_uid = f"_:r{r}"
                room_uids.append(r_uid)
                nquads.append(f'{r_uid} <type> "Room" .')
                nquads.append(f'{r_uid} <name> "Room {r}" .')
                
                # Link to random floor
                parent_floor = floor_uids[r % len(floor_uids)]
                nquads.append(f'{parent_floor} <has_room> {r_uid} .')

            # 3. Create Sensors (The Active Nodes) & Link to Rooms
            for s in range(num_sensors):
                s_id = f"sensor_{s}"
                # Note: In Centralized, these nodes will be updated frequently.
                # In Decentralized, they are static references.
                nquads.append(f'_:s{s} <device_id> "{s_id}" .')
                nquads.append(f'_:s{s} <dgraph.type> "Device" .')
                
                # Link to random room
                parent_room = room_uids[s % len(room_uids)]
                nquads.append(f'{parent_room} <contains_device> _:s{s} .')

            # 4. Create "Noise" Nodes (Assets) to bloat the index size
            # This tests if write speed degrades when the graph is huge.
            for a in range(num_assets):
                nquads.append(f'_:a{a} <type> "Asset" .')
                nquads.append(f'_:a{a} <name> "Desk Chair {a}" .')
                nquads.append(f'_:a{a} <asset_id> "asset_{a}" .')

            # Batch mutation (Dgraph has limits on mutation size, simplified here)
            # In a real heavy setup, we would split this list.
            # We assume it fits for < 10k nodes in this demo.
            mutation = pydgraph.Mutation(set_nquads='\n'.join(nquads).encode('utf-8'))
            txn.mutate(mutation=mutation, commit_now=True)
            
        finally:
            txn.discard()