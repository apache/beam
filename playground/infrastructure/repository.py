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
Module contains methods to work with repository
"""
import logging
import os
from typing import List

from config import RepoProps

from helper import Example


def set_dataset_path_for_examples(examples: List[Example]):
    for example in examples:
        for dataset in example.datasets:
            file_name = f"{dataset.name}.{dataset.format}"
            dataset_path = os.path.join(RepoProps.REPO_DATASETS_PATH, file_name)
            if not os.path.isfile(dataset_path):
                logging.error("File not found at the specified path: %s", dataset_path)
                raise FileNotFoundError
            dataset.path = file_name
