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
#   name: GroupByKey
#   description: Task from katas that groups words by its first letter.
#   multifile: false
#   context_line: 32
#   categories:
#     - Combiners
#   complexity: BASIC
#   tags:
#     - map
#     - group
#     - strings

def groupbykey():
  # [START groupbykey]
  import apache_beam as beam

  with beam.Pipeline() as p:

    (p | beam.Create(['apple', 'ball', 'car', 'bear', 'cheetah', 'ant'])
       | beam.Map(lambda word: (word[0], word))
       | beam.GroupByKey()
       | beam.LogElements())
  # [END groupbykey]

if __name__ == '__main__':
  groupbykey()
