from dpk_repo_level_order.internal.store.ray_store import (
    KeyedValueListActorPool,
    create_pool,
)
from dpk_repo_level_order.internal.store.store import FSStore


# string keys for the required params
store_type_value_s3 = "s3"
store_type_value_local = "local"
store_type_value_ray = "ray"
store_type_value_ray_2 = "ray_store"

store_type_key = "store_type"
store_backend_dir_key = "store_backend_dir"
store_s3_keyid_key = "store_s3_key"
store_s3_secret_key = "store_s3_secret"
store_s3_endpoint_key = "store_s3_endpoint"
s3_creds_key = "s3_creds"
store_ray_cpus_key = "store_ray_cpus"
store_ray_nworkers_key = "store_ray_nworkers"

store_params_key = "store_params"

store_pool_key = "store_pool"


def validate_store_params(store_params):
    allowed_keys = [
        store_type_key,
        store_pool_key,
        store_backend_dir_key,
        store_ray_cpus_key,
        store_ray_nworkers_key,
        store_s3_keyid_key,
        store_s3_endpoint_key,
        store_s3_secret_key,
        s3_creds_key,
    ]
    for k in store_params.keys():
        if k not in allowed_keys:
            raise ValueError(f"Unsupported key: {k} in store_params")
    # required params
    store_type = store_params[store_type_key]
    if store_type in [store_type_value_s3, store_type_value_local]:
        # backedndir is required
        if store_backend_dir_key not in store_params.keys():
            raise ValueError(f"{store_backend_dir_key} not set for {store_type}")
        if store_type is store_type_value_s3:
            # s3 creds are required
            for required in [store_s3_keyid_key, store_s3_endpoint_key, store_s3_secret_key]:
                if required not in store_params.keys():
                    raise ValueError(f"Required key: {required} not set for {store_type}")
    if store_type in [store_type_value_ray]:
        for required in [store_ray_cpus_key, store_ray_nworkers_key, store_pool_key]:
            if required not in store_params.keys():
                raise ValueError(f"Required key: {required} not set for {store_type}")


def create_store_params(captured):
    print("Creating Store Params")
    if captured[store_type_key] == store_type_value_s3:
        store_params = {
            store_backend_dir_key: captured[store_backend_dir_key],
            store_type_key: store_type_value_s3,
        }
    elif captured[store_type_key] == store_type_value_local:
        store_params = {
            store_backend_dir_key: captured[store_backend_dir_key],
            store_type_key: store_type_value_local,
        }
    else:
        store_params = {
            store_ray_cpus_key: captured[store_ray_cpus_key],
            store_ray_nworkers_key: captured[store_ray_nworkers_key],
            store_type_key: store_type_value_ray,
            store_pool_key: None,
        }
    # update store params
    return {"store_params": store_params}


def init_store_params(store_params, logger):
    print("Init Store params")
    try:
        logger.info(store_params.keys())
        if store_params[store_type_key] == store_type_value_ray:
            cpus = store_params[store_ray_cpus_key]
            workers = store_params[store_ray_nworkers_key]
            store_params = store_params | {store_pool_key: create_pool(cpus, workers)}
            return store_params
        if store_params[store_type_key] == store_type_value_s3:
            store_params = store_params | {
                store_s3_keyid_key: store_params[s3_creds_key]["access_key"],
                store_s3_secret_key: store_params[s3_creds_key]["secret_key"],
                store_s3_endpoint_key: store_params[s3_creds_key]["url"],
            }
            return store_params

    except KeyError:
        print("Failed updating store params for ray store backend.")
        raise KeyError
    return store_params


def create_store(store_params):
    if store_params[store_type_key] == store_type_value_ray:
        print("Creating ray based store.")
        processors = store_params[store_pool_key]
        return KeyedValueListActorPool(processors)

    store_backend_dir = store_params[store_backend_dir_key]
    s3_params = None
    if store_params[store_type_key] == store_type_value_local:
        print("Creating local store.")
        s3_params = None

    if store_params[store_type_key] == store_type_value_s3:
        s3_params = {
            "secret_key": store_params[store_s3_secret_key],
            "access_key": store_params[store_s3_keyid_key],
            "endpoint": store_params[store_s3_endpoint_key],
        }
        print(f"Creating S3 Store with {list(s3_params.keys())}")

        store_backend_dir = store_backend_dir.replace("s3://", "")
    return FSStore(store_backend_dir, s3_params)
