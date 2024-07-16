import importlib.util
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import yaml
from spark_transformer_runtime import SparkTransformerRuntime


def load_udf_class(udf_path, udf_class_name):
    udf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), udf_path)
    spec = importlib.util.spec_from_file_location(udf_class_name, udf_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, udf_class_name)


def run_step(step, common_args, common_configs):
    # try:
    UDFClass = load_udf_class(step["udf_path"], step["udf_class"])
    specific_args = step.get("args", {})
    specific_configs = step.get("configs", {})

    # Merge common and specific arguments and heuristics
    args = {**common_args, **specific_args}
    configs = {**common_configs, **specific_configs}

    logging.info(f"Running {step['name']} with args {args} and configs {configs}")
    udf_instance = UDFClass(**args, configs=configs)
    udf_instance.execute()
    # except Exception as e:
    #     logging.error(f"Error in step {step['name']}: {e}")


def setup_logging(log_level):
    log_formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler = logging.StreamHandler()
    handler.setFormatter(log_formatter)

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(handler)


def main(config_path="config.yml"):
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yml")
    with open(config_path, "r") as config_fp:
        config = yaml.safe_load(os.path.expandvars(config_fp.read()))

    logging_level = os.getenv("LOGGING_LEVEL", config["logging_level"]).upper()
    setup_logging(getattr(logging, logging_level, logging.INFO))

    common_args = config.get("common_args", {})
    common_configs = config.get("common_configs", {})

    for step in config["steps"]:
        run_step(step, common_args, common_configs)

    # with ThreadPoolExecutor() as executor:
    #     futures = [executor.submit(run_step, step, common_args, common_heuristics) for step in config['steps']]
    #     for future in futures:
    #         future.result()


if __name__ == "__main__":
    logging.info("Created spark session for generating minhash and band signatures")
    main("config.yaml")
