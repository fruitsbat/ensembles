import os

def is_main_node() -> bool:
    print(os.environ["SLURM_NODEID"])
    return False

def node_id() -> int:
    return int(os.environ["SLURM_NODEID"])