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

# beam-playground:
#   name: additional-outputs
#   description: Additional outputs example.
#   multifile: false
#   context_line: 59
#   categories:
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - hellobeam

import apache_beam as beam
from apache_beam import pvalue

# Output PCollection
class Output(beam.PTransform):
    class _OutputFn(beam.DoFn):
        def __init__(self, prefix=''):
            super().__init__()
            self.prefix = prefix

        def process(self, element):
            print(self.prefix+str(element))

    def __init__(self, label=None,prefix=''):
        super().__init__(label)
        self.prefix = prefix

    def expand(self, input):
        input | beam.ParDo(self._OutputFn(self.prefix))

num_below_100_tag = 'num_below_100'
num_above_100_tag = 'num_above_100'

class ProcessNumbersDoFn(beam.DoFn):

    def process(self, element):
        if element <= 100:
            yield element
        else:
            yield pvalue.TaggedOutput(num_above_100_tag, element)

with beam.Pipeline() as p:
  results = (p | beam.Create([10, 50, 120, 20, 200, 0])
               | beam.ParDo(ProcessNumbersDoFn()).with_outputs(num_above_100_tag, main=num_below_100_tag))

  # First PCollection
  results[num_below_100_tag] | 'Log nums below 100' >> Output(prefix='num_below_100: ')

  # Additional PCollection
  results[num_above_100_tag] | 'Log nums above 100' >> Output(prefix='num_above_100: ')