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
#     name: FinalChallenge1
#     description: Final challenge 1.
#     multifile: false
#     context_line: 50
#     categories:
#       - Quickstart
#     complexity: BASIC
#     tags:
#       - hellobeam


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.transforms.combiners import Mean, TopCombineFn
from operator import itemgetter

pipeline_options = PipelineOptions(['--temp_location', 'input.csv'])

with beam.Pipeline(options=pipeline_options) as p:
    rows = (p | "Read" >> ReadFromText('input.csv')
            | "Parse" >> beam.Map(lambda line: line.split(',')))
