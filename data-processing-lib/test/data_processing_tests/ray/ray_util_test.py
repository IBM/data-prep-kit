import ray
from data_processing.ray import RayUtils, TransformStatistics
from data_processing.utils import GB, TransformUtils
import pyarrow as pa


params = {}
actor_options = {"num_cpus": 1, "memory": GB, "max_task_retries": 10}


def _create_table() -> pa.Table:
    languages = pa.array(
        [
            "Cobol",
            "Java",
            "C",
        ]
    )
    names = ["language"]
    return pa.Table.from_arrays([languages], names=names)


def test_column_validation():
    table = _create_table()
    res = TransformUtils.validate_columns(table=table, required=["foo"])
    assert res == False
    res = TransformUtils.validate_columns(table=table, required=["language"])
    assert res == True


def test_add_column():
    table = _create_table()
    data = ["a", "b", "c"]
    updated = TransformUtils.add_column(table=table, name="language", content=data)
    columns = updated.schema.names
    print(columns)
    assert len(columns) == 1
    updated = TransformUtils.add_column(table=table, name="foo", content=data)
    columns = updated.schema.names
    print(columns)
    assert len(columns) == 2


def test_actor_creation():
    print("Starting Ray cluster")
    ray.init()
    support = RayUtils()

    res = support.get_available_resources()
    print(f"\navailable resources {res}")

    execs = support.create_actors(clazz=TransformStatistics, params=params, actor_options=actor_options, n_actors=1)

    execs[0].add_stats.remote({"source_documents": 1, "source_size": 500})
    execs[0].add_stats.remote({"source_documents": 1, "source_size": 500, "result_documents": 1, "result_size": 300})

    stats = ray.get(execs[0].get_execution_stats.remote())
    print(stats)

    assert 2 == stats["source_documents"]
    assert 1000 == stats["source_size"]
    assert 1 == stats["result_documents"]
    assert 300 == stats["result_size"]

    res1 = support.get_available_resources()
    print(f"available resources {res1}")

    assert 1 == res["cpus"] - res1["cpus"]
    assert 1 == res["memory"] - res1["memory"]

    print("shutting down Ray cluster")
    ray.shutdown()
