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
#   name: Flatten
#   description: Task from katas that merges two PCollections of words into a single PCollection.
#   multifile: false
#   context_line: 33
#   categories:
#     - Flatten
#   complexity: BASIC
#   tags:
#     - merge
#     - strings

def flatten():
  # [START flatten]
  import apache_beam as beam
  
  with beam.Pipeline() as p:
  
    wordsStartingWithA = \
        p | 'Words starting with A' >> beam.Create(['apple', 'ant', 'arrow'])
  
    wordsStartingWithB = \
        p | 'Words starting with B' >> beam.Create(['ball', 'book', 'bow'])
  
    ((wordsStartingWithA, wordsStartingWithB)
      | beam.Flatten()
      | beam.LogElements())
  # [END flatten]

if __name__ == '__main__':
  flatten()
