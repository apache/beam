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

"""A simple wordcount pipeline for FlinkRunner testing."""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import sys

def run():
  print("--- wordcount_flink_test.py: run() started ---", file=sys.stderr)
  print(f"--- wordcount_flink_test.py: PipelineOptions: {sys.argv}", file=sys.stderr)
  options = PipelineOptions()
  with beam.Pipeline(options=options) as p:
    (p | 'Create' >> beam.Create(['hello world', 'beam fun', 'hello beam'])
       | 'Split' >> beam.FlatMap(lambda x: x.split())
       | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
       | 'GroupAndSum' >> beam.CombinePerKey(sum)
       | 'Print' >> beam.Map(lambda x: print(f"--- RESULT: {x} ---", file=sys.stderr)))
  print("--- wordcount_flink_test.py: run() finished ---", file=sys.stderr)

if __name__ == '__main__':
  run()
