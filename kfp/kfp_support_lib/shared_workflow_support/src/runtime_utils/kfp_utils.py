# (C) Copyright IBM Corp. 2024.
# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import json
import os
import re
import sys
from typing import Any

from data_processing.utils import get_logger


logger = get_logger(__name__)


class KFPUtils:
    """
    Helper utilities for KFP implementations
    """

    @staticmethod
    def credentials(
        access_key: str = "S3_KEY", secret_key: str = "S3_SECRET", endpoint: str = "ENDPOINT"
    ) -> tuple[str, str, str]:
        """
        Get credentials from the environment
        :param access_key: environment variable for access key
        :param secret_key: environment variable for secret key
        :param endpoint: environment variable for S3 endpoint
        :return:
        """
        s3_key = os.getenv(access_key, None)
        s3_secret = os.getenv(secret_key, None)
        s3_endpoint = os.getenv(endpoint, None)
        if s3_key is None or s3_secret is None or s3_endpoint is None:
            logger.warning("Failed to load s3 credentials")
        return s3_key, s3_secret, s3_endpoint

    @staticmethod
    def get_namespace() -> str:
        """
        Get k8 namespace that we are running it
        :return:
        """
        ns = ""
        try:
            file = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r")
        except Exception as e:
            logger.warning(
                f"Failed to open /var/run/secrets/kubernetes.io/serviceaccount/namespace file, " f"exception {e}"
            )
        else:
            with file:
                ns = file.read()
        return ns

    @staticmethod
    def runtime_name(ray_name: str = "", run_id: str = "") -> str:
        """
        Get unique runtime name
        :param ray_name:
        :param run_id:
        :return: runtime name
        """
        # K8s objects cannot contain special characters, except '_', All characters should be in lower case.
        if ray_name != "":
            ray_name = ray_name.replace("_", "-").lower()
            pattern = r"[^a-zA-Z0-9-]"  # the ray_name cannot contain upper case here, but leave it just in case.
            ray_name = re.sub(pattern, "", ray_name)
        else:
            ray_name = "a"
        # the return value plus namespace name will be the name of the Ray Route,
        # which length is restricted to 64 characters,
        # therefore we restrict the return name by 15 character.
        if run_id != "":
            return f"{ray_name[:9]}-{run_id[:5]}"
        return ray_name[:15]

    @staticmethod
    def dict_to_req(d: dict[str, Any], executor: str = "transformer_launcher.py") -> str:
        res = f"python {executor} "
        for key, value in d.items():
            if str(value) != "":
                if isinstance(value, str):
                    if '"' in value:
                        logger.warning(
                            f"can't parse inputs with double quotation marks, please use single quotation marks instead"
                        )
                    res += f'--{key}="{value}" '
                else:
                    res += f"--{key}={value} "

        logger.info(f"request to execute: {res}")
        return res

    # Load a string that represents a json to python dictionary
    @staticmethod
    def load_from_json(js: str) -> dict[str, Any]:
        try:
            return json.loads(js)
        except Exception as e:
            logger.warning(f"Failed to load parameters {js} with error {e}")
            sys.exit(1)

    @staticmethod
    def default_compute_execution_params(
        worker_options: str,  # ray worker configuration
        actor_options: str,  # cpus per actor
    ) -> str:
        """
        This is the most simplistic transform execution parameters computation
        :param worker_options: configuration of ray workers
        :param actor_options: actor request requirements
        :return: number of actors
        """
        import sys

        from data_processing.utils import GB, get_logger
        from runtime_utils import KFPUtils

        logger = get_logger(__name__)

        # convert input
        w_options = KFPUtils.load_from_json(worker_options.replace("'", '"'))
        a_options = KFPUtils.load_from_json(actor_options.replace("'", '"'))
        # Compute available cluster resources
        cluster_cpu = w_options["replicas"] * w_options["cpu"]
        cluster_mem = w_options["replicas"] * w_options["memory"]
        cluster_gpu = w_options["replicas"] * w_options.get("gpu", 0.0)
        logger.info(f"Cluster available CPUs {cluster_cpu}, Memory {cluster_mem}, GPUs {cluster_gpu}")
        # compute number of actors
        n_actors_cpu = int((cluster_cpu - 1) * 0.7 / a_options.get("num_cpus", 0.5))
        n_actors_memory = int(cluster_mem * 0.85 / (a_options.get("memory", GB) / GB))
        n_actors = min(n_actors_cpu, n_actors_memory)
        # Check if we need gpu calculations as well
        actor_gpu = a_options.get("num_gpus", 0)
        if actor_gpu > 0:
            n_actors_gpu = int(cluster_gpu / actor_gpu)
            n_actors = min(n_actors, n_actors_gpu)
        logger.info(f"Number of actors - {n_actors}")
        if n_actors < 1:
            logger.warning(
                f"Not enough cpu/gpu/memory to run transform, "
                f"required cpu {a_options.get('num_cpus', .5)}, available {cluster_cpu}, "
                f"required memory {a_options.get('memory', 1)}, available {cluster_mem}, "
                f"required cpu {actor_gpu}, available {cluster_gpu}"
            )
            sys.exit(1)

        return str(n_actors)
