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
import asyncio
import os

from cd_helper import CDHelper
from ci_helper import CIHelper
from helper import find_examples, get_supported_categories
from logger import setup_logger


def ci_step():
    """
    CI step to verify all beam examples/tests/katas
    """
    setup_logger()
    root_dir = os.getenv("BEAM_ROOT_DIR")
    categories_file = os.getenv("BEAM_EXAMPLE_CATEGORIES")
    supported_categories = get_supported_categories(categories_file)
    ci_helper = CIHelper()
    examples = find_examples(root_dir, supported_categories)
    asyncio.run(ci_helper.verify_examples(examples))


def cd_step():
    """
    CD step to save all beam examples/tests/katas and their outputs on the Google Cloud
    """
    setup_logger()
    root_dir = os.getenv("BEAM_ROOT_DIR")
    categories_file = os.getenv("BEAM_EXAMPLE_CATEGORIES")
    supported_categories = get_supported_categories(categories_file)
    cd_helper = CDHelper()
    examples = find_examples(root_dir, supported_categories)
    cd_helper.store_examples(examples)
