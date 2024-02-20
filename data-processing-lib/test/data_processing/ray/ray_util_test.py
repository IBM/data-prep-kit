import ray
from fm_data_processing.ray import *
from fm_data_processing.data_access import *

params = {}
actor_options = {"num_cpus": 1, "memory": GB, "max_task_retries": 10}


def test_actor_creation():
    ray.init()
    support = RayUtils()

    res = support.get_available_resources()
    print(f"\navailable resources {res}")

    execs = support.create_actors(clazz=Statistics, params=params, actor_options=actor_options, n_actors=1)

    execs[0].add_stats.remote({"source_documents": 1, "source_size": 500})
    execs[0].add_stats.remote({"source_documents": 1, "source_size": 500, "result_documents": 1, "result_size": 300})

    stats = ray.get(execs[0].execution_stats.remote())
    print(stats)

    assert 2 == stats["source_documents"]
    assert 1000 == stats["source_size"]
    assert 1 == stats["result_documents"]
    assert 300 == stats["result_size"]

    res1 = support.get_available_resources()
    print(f"available resources {res1}")

    assert 1 == res["cpus"] - res1["cpus"]
    assert 1 == res["memory"] - res1["memory"]
