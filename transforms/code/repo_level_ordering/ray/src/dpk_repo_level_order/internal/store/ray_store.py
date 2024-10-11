import random

import ray
from data_processing_ray.runtime.ray import RayUtils
from ray.util import ActorPool


@ray.remote(scheduling_strategy="SPREAD")
class KeyedValueListActor:
    """
    This class uses ray actor to serve as a dictionary of type `dict[str, List[str]]`
    and provides methods to add/get items to/from it.

    """

    dict_: dict

    def __init__(self, a):
        self.dict_ = {}

    def put(self, key, value):
        try:
            self.dict_[key].append(value)
        except KeyError:
            self.dict_[key] = [value]
        except AttributeError:
            self.dict_ = {}
            self.dict_[key] = [value]

    def put_dict(self, dicta):
        for k, v in dicta.items():
            self.put(k, v)

    def get(self, key):
        try:
            return self.dict_.get(key)
        except:
            return None

    def get_dict(self, x):
        return self.dict_

    def keys(self, x):
        try:
            return self.dict_.keys()
        except:
            return []


class KeyedValueListActorPool:
    """
    This class uses ray actor pool to serve as a dictionary of type `dict[str, List[str]]`
    and provides methods to add/get items to/from it.

    """

    def __init__(self, processors):
        self.pool = ActorPool(processors)
        self.processors = processors
        self.n_workers = len(self.processors)

    def put_dict(self, dicta):
        dict_ = random.choice(self.processors)
        return ray.get(dict_.put_dict.remote(dicta))

    def merge_dicts(self, local_dict, received_dict):
        received_keys = [x for x in received_dict.items()]
        for k, v in received_keys:
            try:
                local_dict[k] = local_dict[k] + v
            except Exception as e:
                local_dict[k] = v
            del received_dict[k]
        local_dict = local_dict | received_dict
        return local_dict

    def put(self, key: str, value: str):
        # may randomly append to an actor
        dict_ = random.choice(self.processors)
        return ray.get(dict_.put.remote(key, value))

    def items(self):
        """returns the keys stored in the store"""
        r = self.pool.map(fn=lambda a, v: a.keys.remote(v), values=self.n_workers * [1])
        result = []
        while self.pool.has_next():
            try:
                data = self.pool.get_next_unordered()
                if data is not None:
                    result = result + [k for k in data]
            except Exception as e:
                print(f"[items()] Error: {e}. Ignoring the error")
                continue
        return list(set(result))

    def items_kv(self):
        """returns the key, value tuple for the keys and values stored in the store"""
        return self._get_dict().items()

    def _get_dict(self):
        # get dicts from all actors first
        r = self.pool.map(fn=lambda a, v: a.get_dict.remote(v), values=self.n_workers * [1])
        result_dict = {}
        while self.pool.has_next():
            try:
                dict_data = self.pool.get_next_unordered()
                if dict_data is not None:
                    result_dict = self.merge_dicts(result_dict, dict_data)
            except Exception as e:
                print(f"[_get_dict()] Error: {e}. Ignoring the error")
                continue
        return result_dict

    def get(self, key):
        self.pool.map_unordered(fn=lambda a, v: a.get.remote(v), values=self.n_workers * [key])

        result = []
        while self.pool.has_next():
            try:
                data = self.pool.get_next_unordered()
                if data is not None:
                    result = result + data
            except Exception as e:
                print(f"[get()] Error: {e}. Ignoring the error")
                continue
        return list(set(result))


def create_pool(cpus: float, actors: int):
    """
    Creates a pool of Ray actors to serve as proxy for
    ray based store.
    """
    processors = RayUtils.create_actors(
        clazz=KeyedValueListActor,
        params=None,
        actor_options={"num_cpus": cpus},
        n_actors=actors,
    )
    return processors
