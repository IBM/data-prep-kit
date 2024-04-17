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

import logging
import os

from data_processing.utils import DPLConfig


def get_log_level(name: str = None):
    if name is None:
        level_name = DPLConfig.DEFAULT_LOG_LEVEL
    else:
        name = name.upper()
        name = "DPL_" + name + "_LOG_LEVEL"
        level_name = os.environ.get(name, DPLConfig.DEFAULT_LOG_LEVEL)
    return level_name


def get_logger(name: str, level=None, file=None):
    logger = logging.getLogger(name)
    if level is None:
        level = get_log_level(name)
    logger.setLevel(level)
    c_handler = logging.StreamHandler()
    if level == "DEBUG":
        # When debugging, include the source link that pycharm understands.
        msgfmt = '%(asctime)s %(levelname)s - %(message)s at "%(pathname)s:%(lineno)d"'
    else:
        msgfmt = "%(asctime)s %(levelname)s - %(message)s"
    timefmt = "%H:%M:%S"

    c_format = logging.Formatter(msgfmt, timefmt)
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    if file is not None:
        f_handler = logging.FileHandler(file)
        f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)

    # Add handlers to the logger
    return logger


# logger = get_logger("main")
# logger.info("info message")
# logger.warning("info message")
# logger.error("info message")
