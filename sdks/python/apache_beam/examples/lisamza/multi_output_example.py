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


class ProcessWords(beam.DoFn):

    def process(self, element):
        # note that an element can go to more than one output (or even no output)
        if len(element) <= 4:
            # Emit this short word to the main output
            yield element
        else:
            # Emit this long word to the 'above_cutoff' output
            yield beam.pvalue.TaggedOutput('above_cutoff', element)
        if element.startswith('s'):
            # Emit this word starting with 's' to the 'starts_with_s' output
            yield beam.pvalue.TaggedOutput('starts_with_s', element)


class ConsolePrint(beam.DoFn):

    def __init__(self, file_name):
        self.file_name = file_name

    def process(self, element):
        print(element, file=open(self.file_name, "a"))


def wait_until_worker_finish():
    logging.info("wait until finish")
    os.environ['CONTROL_API_SERVICE_DESCRIPTOR'] = "url: \"localhost:11441\""
    sdk_worker_main.main(None)
    return runner.PipelineState.DONE


if __name__ == '__main__':
    """Simple example of multi-output ParDo"""
    logging.getLogger().setLevel(logging.WARN)

    cwd = os.getcwd()
    print("cwd:" + cwd)

    options = {"runner": "PortableRunner",
               "experiments": "beam_fn_api",
               "job_endpoint": "localhost:11440"}

    pipeline_options = PipelineOptions.from_dictionary(options)
    p = Pipeline(options=pipeline_options)

    below, above, marked = (p
                            | 'read' >> beam.io.ReadFromText("gatsby.txt")
                            | 'split' >> beam.FlatMap(lambda line: line.split(" "))
                            | 'refine' >> beam.Map(lambda word: re.sub('[^A-Za-z]', '', word))
                            | 'process' >> beam.ParDo(ProcessWords()).with_outputs('above_cutoff',
                                                                                   'starts_with_s',
                                                                                   main='below_cutoff_strings'))

    # pipe the different outputs to corresponding files
    (below
     | "output below" >> beam.ParDo(ConsolePrint("below.txt")))
    (above
     | "output above" >> beam.ParDo(ConsolePrint("above.txt")))
    (marked
     | "output marked" >> beam.ParDo(ConsolePrint("marked.txt")))

    p.run()
    wait_until_worker_finish()
