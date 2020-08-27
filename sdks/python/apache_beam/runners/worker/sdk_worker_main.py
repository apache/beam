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

# pytype: skip-file

from __future__ import absolute_import

import http.server
import json
import logging
import os
import re
import sys
import threading
import traceback
from builtins import object

from google.protobuf import text_format  # type: ignore # not in typeshed

from apache_beam.internal import pickler
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import ProfilingOptions
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.internal import names
from apache_beam.runners.worker.log_handler import FnApiLogRecordHandler
from apache_beam.runners.worker.sdk_worker import SdkHarness
from apache_beam.runners.worker.worker_status import thread_dump
from apache_beam.utils import profiler

# This module is experimental. No backwards-compatibility guarantees.

_LOGGER = logging.getLogger(__name__)


class StatusServer(object):
  def start(self, status_http_port=0):
    """Executes the serving loop for the status server.

    Args:
      status_http_port(int): Binding port for the debug server.
        Default is 0 which means any free unsecured port
    """
    class StatusHttpHandler(http.server.BaseHTTPRequestHandler):
      """HTTP handler for serving stacktraces of all threads."""
      def do_GET(self):  # pylint: disable=invalid-name
        """Return all thread stacktraces information for GET request."""
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()

        self.wfile.write(thread_dump().encode('utf-8'))

      def log_message(self, f, *args):
        """Do not log any messages."""
        pass

    self.httpd = httpd = http.server.HTTPServer(('localhost', status_http_port),
                                                StatusHttpHandler)
    _LOGGER.info(
        'Status HTTP server running at %s:%s',
        httpd.server_name,
        httpd.server_port)

    httpd.serve_forever()


def main(unused_argv):
  """Main entry point for SDK Fn Harness."""
  if 'LOGGING_API_SERVICE_DESCRIPTOR' in os.environ:
    try:
      logging_service_descriptor = endpoints_pb2.ApiServiceDescriptor()
      text_format.Merge(
          os.environ['LOGGING_API_SERVICE_DESCRIPTOR'],
          logging_service_descriptor)

      # Send all logs to the runner.
      fn_log_handler = FnApiLogRecordHandler(logging_service_descriptor)
      # TODO(BEAM-5468): This should be picked up from pipeline options.
      logging.getLogger().setLevel(logging.INFO)
      logging.getLogger().addHandler(fn_log_handler)
      _LOGGER.info('Logging handler created.')
    except Exception:
      _LOGGER.error(
          "Failed to set up logging handler, continuing without.",
          exc_info=True)
      fn_log_handler = None
  else:
    fn_log_handler = None

  # Start status HTTP server thread.
  thread = threading.Thread(
      name='status_http_server', target=StatusServer().start)
  thread.daemon = True
  thread.setName('status-server-demon')
  thread.start()

  if 'PIPELINE_OPTIONS' in os.environ:
    sdk_pipeline_options = _parse_pipeline_options(
        os.environ['PIPELINE_OPTIONS'])
  else:
    sdk_pipeline_options = PipelineOptions.from_dictionary({})

  if 'SEMI_PERSISTENT_DIRECTORY' in os.environ:
    semi_persistent_directory = os.environ['SEMI_PERSISTENT_DIRECTORY']
  else:
    semi_persistent_directory = None

  _LOGGER.info('semi_persistent_directory: %s', semi_persistent_directory)
  _worker_id = os.environ.get('WORKER_ID', None)

  try:
    _load_main_session(semi_persistent_directory)
  except Exception:  # pylint: disable=broad-except
    exception_details = traceback.format_exc()
    _LOGGER.error(
        'Could not load main session: %s', exception_details, exc_info=True)

  try:
    _LOGGER.info(
        'Python sdk harness started with pipeline_options: %s',
        sdk_pipeline_options.get_all_options(drop_default=True))
    control_service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    status_service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    text_format.Merge(
        os.environ['CONTROL_API_SERVICE_DESCRIPTOR'],
        control_service_descriptor)
    if 'STATUS_API_SERVICE_DESCRIPTOR' in os.environ:
      text_format.Merge(
          os.environ['STATUS_API_SERVICE_DESCRIPTOR'],
          status_service_descriptor)
    # TODO(robertwb): Support authentication.
    assert not control_service_descriptor.HasField('authentication')

    experiments = sdk_pipeline_options.view_as(DebugOptions).experiments or []
    enable_heap_dump = 'enable_heap_dump' in experiments
    SdkHarness(
        control_address=control_service_descriptor.url,
        status_address=status_service_descriptor.url,
        worker_id=_worker_id,
        state_cache_size=_get_state_cache_size(experiments),
        data_buffer_time_limit_ms=_get_data_buffer_time_limit_ms(experiments),
        profiler_factory=profiler.Profile.factory_from_options(
            sdk_pipeline_options.view_as(ProfilingOptions)),
        enable_heap_dump=enable_heap_dump).run()
    _LOGGER.info('Python sdk harness exiting.')
  except:  # pylint: disable=broad-except
    _LOGGER.exception('Python sdk harness failed: ')
    raise
  finally:
    if fn_log_handler:
      fn_log_handler.close()


def _parse_pipeline_options(options_json):
  options = json.loads(options_json)
  # Check the options field first for backward compatibility.
  if 'options' in options:
    return PipelineOptions.from_dictionary(options.get('options'))
  else:
    # Remove extra urn part from the key.
    portable_option_regex = r'^beam:option:(?P<key>.*):v1$'
    return PipelineOptions.from_dictionary({
        re.match(portable_option_regex, k).group('key') if re.match(
            portable_option_regex, k) else k: v
        for k,
        v in options.items()
    })


def _get_state_cache_size(experiments):
  """Defines the upper number of state items to cache.

  Note: state_cache_size is an experimental flag and might not be available in
  future releases.

  Returns:
    an int indicating the maximum number of items to cache.
      Default is 0 (disabled)
  """

  for experiment in experiments:
    # There should only be 1 match so returning from the loop
    if re.match(r'state_cache_size=', experiment):
      return int(
          re.match(r'state_cache_size=(?P<state_cache_size>.*)',
                   experiment).group('state_cache_size'))
  return 0


def _get_data_buffer_time_limit_ms(experiments):
  """Defines the time limt of the outbound data buffering.

  Note: data_buffer_time_limit_ms is an experimental flag and might
  not be available in future releases.

  Returns:
    an int indicating the time limit in milliseconds of the the outbound
      data buffering. Default is 0 (disabled)
  """

  for experiment in experiments:
    # There should only be 1 match so returning from the loop
    if re.match(r'data_buffer_time_limit_ms=', experiment):
      return int(
          re.match(
              r'data_buffer_time_limit_ms=(?P<data_buffer_time_limit_ms>.*)',
              experiment).group('data_buffer_time_limit_ms'))
  return 0


def _load_main_session(semi_persistent_directory):
  """Loads a pickled main session from the path specified."""
  if semi_persistent_directory:
    session_file = os.path.join(
        semi_persistent_directory, 'staged', names.PICKLED_MAIN_SESSION_FILE)
    if os.path.isfile(session_file):
      pickler.load_session(session_file)
    else:
      _LOGGER.warning(
          'No session file found: %s. Functions defined in __main__ '
          '(interactive session) may fail.',
          session_file)
  else:
    _LOGGER.warning(
        'No semi_persistent_directory found: Functions defined in __main__ '
        '(interactive session) may fail.')


if __name__ == '__main__':
  main(sys.argv)
