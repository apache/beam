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
Module implements check to define if it is needed to run CI step for Beam
Playground examples

Returns exit code 11 if no examples found

All paths are relative to BEAM_ROOT_DIR
"""
import argparse
import logging
import os
import sys
from pathlib import PurePath
from typing import List

from api.v1.api_pb2 import Sdk
from config import Config
from helper import get_tag, load_supported_categories
from constants import BEAM_EXAMPLE_CATEGORIES_ENV_VAR_KEY, BEAM_ROOT_DIR_ENV_VAR_KEY


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sdk", type=str, choices=Sdk.keys())
    parser.add_argument(
        "--allowlist",
        nargs="*",
        default=[],
        required=True,
        type=PurePath,
        help="if any path falls in here, return success",
    )
    parser.add_argument(
        "--paths",
        nargs="*",
        default=[],
        required=True,
        type=PurePath,
        help="paths to check",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


def check_in_allowlist(paths: List[PurePath], allowlist: List[PurePath]) -> bool:
    """Check if any of allowlist paths affected"""
    for path in paths:
        logging.debug("check if allowlisted: %s", path)
        for w in allowlist:
            if w in [path, *path.parents]:
                logging.info(f"{path} is allowlisted by {w}")
                return True
    return False


def check_sdk_examples(paths: List[PurePath], sdk: Sdk, root_dir: str) -> bool:
    """
    Determine if any of the files is an example of a given SDK
    - has appropriate suffix: *.(go|java|python|scala)
    - has an embedded beam-playground yaml tag
    """
    for path in paths:
        logging.debug("check for example tag: %s", path)
        if path.suffix.lstrip(".") != Config.SDK_TO_EXTENSION[sdk]:
            continue
        path = PurePath(root_dir, path)
        if not os.path.isfile(path):
            # TODO file is deleted but this potentially can break multi file examples
            logging.info(f"{path} not exists, continue")
            continue
        if get_tag(path) is not None:
            logging.info(f"{path} is an example, return")
            return True
    return False

def main():
    args = parse_args()

    root_dir = os.getenv(BEAM_ROOT_DIR_ENV_VAR_KEY)
    if root_dir is None:
        raise KeyError(f"{BEAM_ROOT_DIR_ENV_VAR_KEY} environment variable should be specified in os")
    categories_file = os.getenv(BEAM_EXAMPLE_CATEGORIES_ENV_VAR_KEY)
    if categories_file is None:
        raise KeyError(f"{BEAM_EXAMPLE_CATEGORIES_ENV_VAR_KEY} environment variable should be specified in os")

    load_supported_categories(categories_file)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.WARNING)

    if check_in_allowlist(args.paths, args.allowlist):
        return

    if check_sdk_examples(args.paths, Sdk.Value(args.sdk), root_dir):
        return

    sys.exit(11)


if __name__ == "__main__":
    main()
