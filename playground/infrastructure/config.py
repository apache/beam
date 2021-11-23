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

import os
from dataclasses import dataclass
from api.v1.api_pb2 import STATUS_VALIDATION_ERROR, STATUS_ERROR, STATUS_PREPARATION_ERROR, STATUS_COMPILE_ERROR, \
    STATUS_RUN_TIMEOUT, STATUS_RUN_ERROR, SDK_JAVA, SDK_GO, SDK_PYTHON


@dataclass(frozen=True)
class Config:
    SERVER_ADDRESS = os.getenv("SERVER_ADDRESS", "localhost:8080")
    SUPPORTED_SDK = {"java": SDK_JAVA, "go": SDK_GO, "py": SDK_PYTHON}
    BUCKET_NAME = "test_public_bucket_akvelon"
    TEMP_FOLDER = "temp"
    EXTENSIONS = {"SDK_JAVA": "java", "SDK_GO": "go", "SDK_PYTHON": "py"}
    NO_STORE = "no-store"
    ERROR_STATUSES = [STATUS_VALIDATION_ERROR, STATUS_ERROR, STATUS_PREPARATION_ERROR, STATUS_COMPILE_ERROR,
                      STATUS_RUN_TIMEOUT, STATUS_RUN_ERROR]
    BEAM_PLAYGROUND_TITLE = "Beam-playground:\n"
    BEAM_PLAYGROUND = "Beam-playground"


@dataclass(frozen=True)
class TagFields:
    NAME: str = "name"
    DESCRIPTION: str = "description"
    MULTIFILE: str = "multifile"
    CATEGORIES: str = "categories"
