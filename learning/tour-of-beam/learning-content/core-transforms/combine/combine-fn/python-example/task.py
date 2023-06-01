# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import apache_beam as beam

# beam-playground:
#   name: combine-fn
#   description: Combine-fn example.
#   multifile: false
#   context_line: 64
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam

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

class AverageFn(beam.CombineFn):

    def create_accumulator(self):
        return 0.0, 0

    def add_input(self, accumulator, element):
        (sum, count) = accumulator
        return sum + element, count + 1

    def merge_accumulators(self, accumulators):
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)

    def extract_output(self, accumulator):
        (sum, count) = accumulator
        return sum / count if count else float('NaN')


with beam.Pipeline() as p:
  (p | beam.Create([10, 20, 50, 70, 90])
     | beam.CombineGlobally(AverageFn())
     | Output())