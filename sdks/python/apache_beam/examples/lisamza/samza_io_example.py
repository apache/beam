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

import logging
import os
import re

import apache_beam as beam
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners import runner
from apache_beam.runners.worker import sdk_worker_main


class ConsolePrint(beam.DoFn):
    def process(self, element):
        print(element)
        print(element, file=open("output.txt", "a"))


def wait_until_worker_finish():
    logging.info("wait until finish")
    os.environ['CONTROL_API_SERVICE_DESCRIPTOR'] = "url: \"localhost:11441\""
    sdk_worker_main.main(None)
    return runner.PipelineState.DONE


if __name__ == '__main__':
    """Simple example for word count"""
    logging.getLogger().setLevel(logging.WARN)

    cwd = os.getcwd()
    print("cwd:" + cwd)

    options = {"runner": "PortableRunner",
               "experiments": "beam_fn_api",
               "job_endpoint": "localhost:11440"}

    pipeline_options = PipelineOptions.from_dictionary(options)
    p = Pipeline(options=pipeline_options)

    (p
     | 'read' >> beam.io.ReadFromText("gatsby.txt")
     | 'split' >> beam.FlatMap(lambda line: line.split(" "))
     | 'refine' >> beam.Map(lambda word: re.sub('[^A-Za-z]', '', word).lower())
     | 'count' >> beam.Map(lambda word: (word, 1))
     | 'compute' >> beam.CombinePerKey(beam.combiners.CountCombineFn())
     | 'write' >> beam.ParDo(ConsolePrint()))
    # | 'write' >> beam.io.WriteToKafka("outputTopic"))

    p.run()
    wait_until_worker_finish()
