#
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
#

"""SDK Fn Harness entry point."""

import json
import logging
import os
import sys
import traceback

from google.protobuf import text_format

from apache_beam.internal import pickler
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.dataflow.internal import names
from apache_beam.runners.worker.log_handler import FnApiLogRecordHandler
from apache_beam.runners.worker.sdk_worker import SdkHarness

# This module is experimental. No backwards-compatibility guarantees.


def main(unused_argv):
  """Main entry point for SDK Fn Harness."""
  if 'LOGGING_API_SERVICE_DESCRIPTOR' in os.environ:
    logging_service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    text_format.Merge(os.environ['LOGGING_API_SERVICE_DESCRIPTOR'],
                      logging_service_descriptor)

    # Send all logs to the runner.
    fn_log_handler = FnApiLogRecordHandler(logging_service_descriptor)
    # TODO(vikasrk): This should be picked up from pipeline options.
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(fn_log_handler)
  else:
    fn_log_handler = None

  if 'PIPELINE_OPTIONS' in os.environ:
    sdk_pipeline_options = json.loads(os.environ['PIPELINE_OPTIONS'])
  else:
    sdk_pipeline_options = {}

  if 'SEMI_PERSISTENT_DIRECTORY' in os.environ:
    semi_persistent_directory = os.environ['SEMI_PERSISTENT_DIRECTORY']
  else:
    semi_persistent_directory = None

  logging.info('semi_persistent_directory: %s', semi_persistent_directory)

  try:
    _load_main_session(semi_persistent_directory)
  except Exception:  # pylint: disable=broad-except
    exception_details = traceback.format_exc()
    logging.error(
        'Could not load main session: %s', exception_details, exc_info=True)

  try:
    logging.info('Python sdk harness started with pipeline_options: %s',
                 sdk_pipeline_options)
    service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    text_format.Merge(os.environ['CONTROL_API_SERVICE_DESCRIPTOR'],
                      service_descriptor)
    # TODO(robertwb): Support credentials.
    assert not service_descriptor.oauth2_client_credentials_grant.url
    SdkHarness(service_descriptor.url).run()
    logging.info('Python sdk harness exiting.')
  except:  # pylint: disable=broad-except
    logging.exception('Python sdk harness failed: ')
    raise
  finally:
    if fn_log_handler:
      fn_log_handler.close()


def _load_main_session(semi_persistent_directory):
  """Loads a pickled main session from the path specified."""
  if semi_persistent_directory:
    session_file = os.path.join(
        semi_persistent_directory, 'staged', names.PICKLED_MAIN_SESSION_FILE)
    if os.path.isfile(session_file):
      pickler.load_session(session_file)
    else:
      logging.warning(
          'No session file found: %s. Functions defined in __main__ '
          '(interactive session) may fail.', session_file)
  else:
    logging.warning(
        'No semi_persistent_directory found: Functions defined in __main__ '
        '(interactive session) may fail.')


if __name__ == '__main__':
  main(sys.argv)
