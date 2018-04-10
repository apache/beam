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

import argparse
import logging
import sys
import time

from apache_beam.runners.portability import universal_local_runner


def run(argv):
  if argv[0] == __file__:
    argv = argv[1:]
  parser = argparse.ArgumentParser()
  parser.add_argument('-p', '--port',
                      type=int,
                      help='port on which to serve the job api')
  parser.add_argument('--worker_command_line',
                      help='command line for starting up a worker process')
  options = parser.parse_args(argv)
  job_servicer = universal_local_runner.JobServicer(options.worker_command_line)
  port = job_servicer.start_grpc(options.port)
  while True:
    logging.info("Listening for jobs at %d", port)
    time.sleep(300)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run(sys.argv)
