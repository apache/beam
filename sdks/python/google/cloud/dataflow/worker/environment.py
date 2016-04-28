# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Python Dataflow worker environment compatiblity checking."""

import json
import logging

from google.cloud.dataflow import version


def check_sdk_compatibility(environment_info_path):
  """Checks if the SDK is compatible with the container in which it runs.

  Args:
    environment_info_path: Path to a file in JSON format. The file is expected
      to contain a dictionary with at least two properties: 'language'
      and 'version'.

  Raises:
    RuntimeError: For version or language mismatches.

  Other exceptions can be raised if the environment file is not present or does
  not have the right contents. This can happen only if the base container was
  not built correctly.
  """
  logging.info('Checking if container and SDK language and versions match ...')
  with open(environment_info_path) as f:
    info = json.loads(f.read())
  if info['language'] != 'python':
    message = (
        'SDK language \'python\' does not match container language \'%s\'. '
        'Please rebuild the container using a matching language container.' % (
            info['language']))
    logging.error(message)
    raise RuntimeError(message)
  if info['version'] != version.__version__:
    message = (
        'SDK version %s does not match container version %s. '
        'Please rebuild the container or use a matching version '
        'of the SDK.' % (
            version.__version__, info['version']))
    logging.error(message)
    raise RuntimeError(message)
