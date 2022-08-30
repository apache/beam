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
Module implements CI/CD steps for Beam Playground examples
"""
import argparse
import asyncio
import logging
import os
from typing import List

import config
from api.v1.api_pb2 import Sdk
from cd_helper import CDHelper
from ci_helper import CIHelper
from helper import find_examples, get_supported_categories, Example, validate_examples_for_duplicates_by_name
from logger import setup_logger

parser = argparse.ArgumentParser(
    description="CI/CD Steps for Playground objects")
parser.add_argument(
    "--step",
    dest="step",
    required=True,
    help="CI step to verify all beam examples/tests/katas. CD step to save all "
         "beam examples/tests/katas and their outputs on the GCD",
    choices=[config.Config.CI_STEP_NAME, config.Config.CD_STEP_NAME])
parser.add_argument(
    "--sdk",
    dest="sdk",
    required=True,
    help="Supported SDKs",
    choices=config.Config.SUPPORTED_SDK)

root_dir = os.getenv("BEAM_ROOT_DIR")
categories_file = os.getenv("BEAM_EXAMPLE_CATEGORIES")


def _ci_step(examples: List[Example]):
    """
    CI step to verify single-file beam examples/tests/katas
    """

    ci_helper = CIHelper()
    asyncio.run(ci_helper.verify_examples(examples))


def _cd_step(examples: List[Example], sdk: Sdk):
    """
    CD step to save all beam examples/tests/katas and their outputs on the GCD
    """
    cd_helper = CDHelper()
    cd_helper.save_examples(examples, sdk)


def _check_envs():
    if root_dir is None:
        raise KeyError(
            "BEAM_ROOT_DIR environment variable should be specified in os")
    if categories_file is None:
        raise KeyError(
            "BEAM_EXAMPLE_CATEGORIES environment variable should be specified in os"
        )


def _run_ci_cd(step: config.Config.CI_CD_LITERAL, sdk: Sdk):
    supported_categories = get_supported_categories(categories_file)
    logging.info("Start of searching Playground examples ...")
    examples = find_examples(root_dir, supported_categories, sdk)
    validate_examples_for_duplicates_by_name(examples)
    logging.info("Finish of searching Playground examples")
    logging.info("Number of found Playground examples: %s", len(examples))

    if step == config.Config.CI_STEP_NAME:
        logging.info(
            "Start of verification only single_file Playground examples ...")
        _ci_step(examples=examples)
        logging.info("Finish of verification single_file Playground examples")
    if step == config.Config.CD_STEP_NAME:
        logging.info("Start of saving Playground examples ...")
        _cd_step(examples=examples, sdk=sdk)
        logging.info("Finish of saving Playground examples")


if __name__ == "__main__":
    parser = parser.parse_args()
    _check_envs()
    setup_logger()
    _run_ci_cd(parser.step, Sdk.Value(parser.sdk))
