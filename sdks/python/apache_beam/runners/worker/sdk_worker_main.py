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

from google.protobuf import text_format

from apache_beam.internal import pickler
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.portability.api import endpoints_pb2
from apache_beam.runners.internal import names
from apache_beam.runners.worker.log_handler import FnApiLogRecordHandler
from apache_beam.runners.worker.sdk_worker import SdkHarness
from apache_beam.utils import profiler

# This module is experimental. No backwards-compatibility guarantees.


class StatusServer(object):

  @classmethod
  def get_thread_dump(cls):
    lines = []
    frames = sys._current_frames()  # pylint: disable=protected-access

    for t in threading.enumerate():
      lines.append('--- Thread #%s name: %s ---\n' % (t.ident, t.name))
      lines.append(''.join(traceback.format_stack(frames[t.ident])))

    return lines

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

        for line in StatusServer.get_thread_dump():
          self.wfile.write(line)

      def log_message(self, f, *args):
        """Do not log any messages."""
        pass

    self.httpd = httpd = http.server.HTTPServer(
        ('localhost', status_http_port), StatusHttpHandler)
    logging.info('Status HTTP server running at %s:%s', httpd.server_name,
                 httpd.server_port)

    httpd.serve_forever()


def main(unused_argv):
  """Main entry point for SDK Fn Harness."""
  if 'LOGGING_API_SERVICE_DESCRIPTOR' in os.environ:
    logging_service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    text_format.Merge(os.environ['LOGGING_API_SERVICE_DESCRIPTOR'],
                      logging_service_descriptor)

    # Send all logs to the runner.
    fn_log_handler = FnApiLogRecordHandler(logging_service_descriptor)
    # TODO(BEAM-5468): This should be picked up from pipeline options.
    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().addHandler(fn_log_handler)
    logging.info('Logging handler created.')
  else:
    fn_log_handler = None

  # Start status HTTP server thread.
  thread = threading.Thread(target=StatusServer().start)
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

  logging.info('semi_persistent_directory: %s', semi_persistent_directory)

  try:
    _load_main_session(semi_persistent_directory)
  except Exception:  # pylint: disable=broad-except
    exception_details = traceback.format_exc()
    logging.error(
        'Could not load main session: %s', exception_details, exc_info=True)

  try:
    logging.info('Python sdk harness started with pipeline_options: %s',
                 sdk_pipeline_options.get_all_options(drop_default=True))
    service_descriptor = endpoints_pb2.ApiServiceDescriptor()
    text_format.Merge(os.environ['CONTROL_API_SERVICE_DESCRIPTOR'],
                      service_descriptor)
    # TODO(robertwb): Support credentials.
    assert not service_descriptor.oauth2_client_credentials_grant.url
    SdkHarness(
        control_address=service_descriptor.url,
        worker_count=_get_worker_count(sdk_pipeline_options),
        profiler_factory=profiler.Profile.factory_from_options(
            sdk_pipeline_options.view_as(pipeline_options.ProfilingOptions))
    ).run()
    logging.info('Python sdk harness exiting.')
  except:  # pylint: disable=broad-except
    logging.exception('Python sdk harness failed: ')
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
        re.match(portable_option_regex, k).group('key')
        if re.match(portable_option_regex, k) else k: v
        for k, v in options.items()
    })


def _get_worker_count(pipeline_options):
  """Extract worker count from the pipeline_options.

  This defines how many SdkWorkers will be started in this Python process.
  And each SdkWorker will have its own thread to process data. Name of the
  experimental parameter is 'worker_threads'
  Example Usage in the Command Line:
    --experimental worker_threads=1

  Note: worker_threads is an experimental flag and might not be available in
  future releases.

  Returns:
    an int containing the worker_threads to use. Default is 1
  """
  experiments = pipeline_options.view_as(DebugOptions).experiments

  experiments = experiments if experiments else []

  for experiment in experiments:
    # There should only be 1 match so returning from the loop
    if re.match(r'worker_threads=', experiment):
      return int(
          re.match(r'worker_threads=(?P<worker_threads>.*)',
                   experiment).group('worker_threads'))

  return 12


def _load_main_session(semi_persistent_directory):
  """Loads a pickled main session from the path specified."""
  if semi_persistent_directory:
    session_file = os.path.join(semi_persistent_directory, 'staged',
                                names.PICKLED_MAIN_SESSION_FILE)
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
