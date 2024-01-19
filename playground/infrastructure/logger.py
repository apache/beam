# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Log configurator. Adds required handlers and configures log format.
"""

import logging
import sys
from logging import INFO, WARNING, ERROR, CRITICAL


def setup_logger():
    """
    Setup logging.

    Add 2 handler in root logger:
        StreamHandler - for logs(INFO and WARNING levels) to the stdout
        StreamHandler - for logs(ERROR and CRITICAL levels) to the stderr
    """
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s'
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.addFilter(lambda record: record.levelno in (WARNING,))
    stdout_handler.setFormatter(formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.addFilter(lambda record: record.levelno in (ERROR, CRITICAL))
    stderr_handler.setFormatter(formatter)

    log.addHandler(stdout_handler)
    log.addHandler(stderr_handler)
