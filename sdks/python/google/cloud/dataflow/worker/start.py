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

"""Python Dataflow started script for the worker main loop."""
# Import _strptime to work around http://bugs.python.org/issue7980.  There is a
# thread-safety issue with datetime.datetime.strptime if this module is not
# already imported.
import _strptime  # pylint: disable=unused-import
import logging
import random
import re
import sys

from google.cloud.dataflow.worker import batchworker
from google.cloud.dataflow.worker import logger


def parse_properties(args):
  properties = {}
  unused_args = []
  for arg in args:
    match = re.search(r'-D(.+)=(.+)', arg)
    if match:
      properties[match.group(1)] = match.group(2)
    else:
      unused_args.append(arg)
  return properties, unused_args


def main():
  properties, unused_args = parse_properties(sys.argv[1:])

  # Initialize the logging machinery.
  job_id = properties['job_id']
  worker_id = properties['worker_id']
  log_path = properties['dataflow.worker.logging.location']
  logger.initialize(job_id, worker_id, log_path)

  logging.info('Worker started with properties: %s', properties)

  if unused_args:
    logging.warning('Unrecognized arguments %s', unused_args)

  if properties.get('is_streaming', False):
    # TODO(ccy): right now, if we pull this in when not in the worker
    # environment, this will fail on not being able to pull in the correct gRPC
    # C dependencies.  I am investigating a fix.
    from google.cloud.dataflow.worker import streamingworker  # pylint: disable=g-import-not-at-top
    # Initialize the random number generator, which is used to generate Windmill
    # client IDs.
    random.seed()
    logging.info('Starting streaming worker.')
    streamingworker.StreamingWorker(properties).run()
  else:
    logging.info('Starting batch worker.')
    batchworker.BatchWorker(properties).run()


if __name__ == '__main__':
  main()
