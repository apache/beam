#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

#   beam-playground:
#     name: FinalChallenge3
#     description: Final challenge 3.
#     multifile: false
#     context_line: 50
#     categories:
#       - Quickstart
#     complexity: BASIC
#     tags:
#       - hellobeam

import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms import DoFn, ParDo, WindowInto, FixedWindows
from apache_beam.transforms.window import AfterProcessingTime


def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        shakespeare = (p
                       | "Read Text" >> ReadFromText("gs://apache-beam-samples/shakespeare/kinglear.txt")
                       )

if __name__ == '__main__':
    run()
