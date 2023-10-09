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
#   name: FilterParDo
#   description: Task from katas to implement a filter function that filters out the even numbers.
#   multifile: false
#   context_line: 39
#   categories:
#     - Filtering
#   complexity: BASIC
#   tags:
#     - filter
#     - numbers

import apache_beam as beam


class FilterOutEvenNumber(beam.DoFn):

    def process(self, element):
        if element % 2 == 1:
            yield element


with beam.Pipeline() as p:
  (p | beam.Create(range(1, 11))
     | beam.ParDo(FilterOutEvenNumber())
     | beam.LogElements())
