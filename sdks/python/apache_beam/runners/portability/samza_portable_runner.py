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

from __future__ import absolute_import

import logging
import os

from apache_beam.options.pipeline_options import PortableOptions
from apache_beam.runners import runner
from apache_beam.runners.portability.portable_runner import PortableRunner
from apache_beam.runners.worker import sdk_worker_main


class SamzaPortableRunner(PortableRunner):

    def __init__(self, is_embedded_fnapi_runner=False):
        self.is_embedded_fnapi_runner = is_embedded_fnapi_runner

    def run_pipeline(self, pipeline):
        job_endpoint = pipeline.options.view_as(PortableOptions).job_endpoint
        control_endpoint = pipeline.options.view_as(PortableOptions).control_endpoint
        os.environ['CONTROL_API_SERVICE_DESCRIPTOR'] = "url: \"" + control_endpoint + "\""
        if not job_endpoint:
            # TODO: in this case, start the java job service from Python
            pass
        PortableRunner.run_pipeline(self, pipeline)
        return SamzaPipelineResult(None)


class SamzaPipelineResult(runner.PipelineResult):
    def wait_until_finish(self):
        logging.info("wait until finish")
        sdk_worker_main.main(None)
        return runner.PipelineState.DONE
