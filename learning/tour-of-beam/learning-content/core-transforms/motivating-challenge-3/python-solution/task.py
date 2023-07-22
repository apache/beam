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
#   name: CoreTransformsSolution3
#   description: Core Transforms third motivating solution.
#   multifile: false
#   context_line: 82
#   categories:
#     - Quickstart
#   complexity: BASIC
#   tags:
#     - hellobeam

import apache_beam as beam
from apache_beam import pvalue

wordWithUpperCase = 'wordWithUpperCase'
wordWithLowerCase = 'wordWithLowerCase'

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



class StartWithLetter(beam.DoFn):
    def __init__(self, letter=''):
        self.letter = letter
    def process(self, element):
        if(element.lower().startswith(self.letter)):
            yield element

class ExtractAndCountWord(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll
                | beam.ParDo(StartWithLetter('i'))
                | beam.combiners.Count.PerElement()
                | beam.Map(lambda kv: kv[0])
                )

class ProcessNumbersDoFn(beam.DoFn):
    def process(self, element):
        if element.startswith('I'):
            yield element
        else:
            yield pvalue.TaggedOutput(wordWithLowerCase, element)

class EnrichCountryDoFn(beam.DoFn):
    def process(self, element, wordWithLowerCase):
        if(element.lower() in wordWithLowerCase):
            yield element



with beam.Pipeline() as p:
  results = (p | 'Log words' >> beam.io.ReadFromText('gs://apache-beam-samples/shakespeare/kinglear.txt') \
            | beam.combiners.Sample.FixedSizeGlobally(100) \
            | beam.FlatMap(lambda line: line) \
            | beam.FlatMap(lambda sentence: sentence.split()) \
            | beam.Filter(lambda word: not word.isspace() or word.isalnum()) \
            | ExtractAndCountWord()
            | beam.ParDo(ProcessNumbersDoFn()).with_outputs(wordWithLowerCase, main=wordWithUpperCase))

  results[wordWithUpperCase] | beam.ParDo(EnrichCountryDoFn(),beam.pvalue.AsList(results[wordWithLowerCase])) | Output()