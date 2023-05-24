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
#     name: FinalSolution2
#     description: Final challenge solution 2.
#     multifile: false
#     context_line: 57
#     categories:
#       - Quickstart
#     complexity: BASIC
#     tags:
#       - hellobeam

import apache_beam as beam
import logging
import re

class ExtractDataFn(beam.DoFn):
    def process(self, element):
        element = element.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
        try:
            yield {
                'id': int(element[0]),
                'date': element[1],
                'productId': element[2],
                'productName': element[3],
                'price': float(element[4]),
                'quantity': int(element[5]),
                'customerId': int(element[6]),
                'country': element[7]
            }
        except Exception:
            logging.info("Skipping header")

class PartitionTransactions(beam.DoFn):
    def process(self, element):
        if element['price'] > 10:
            yield beam.pvalue.TaggedOutput('biggerThan10', element)
        else:
            yield beam.pvalue.TaggedOutput('smallerThan10', element)

def run():
    with beam.Pipeline() as pipeline:
        transactions = (pipeline
                        | 'Read from text file' >> beam.io.ReadFromText('input.csv')
                        | 'Extract Data' >> beam.ParDo(ExtractDataFn())
                        )

        partition = (transactions
                     | 'Partition transactions' >> beam.ParDo(PartitionTransactions()).with_outputs('biggerThan10', 'smallerThan10', main='main')
                     )

        biggerThan10 = partition.biggerThan10
        smallerThan10 = partition.smallerThan10

        (biggerThan10
         | 'Calculate sum for biggerThan10' >> beam.CombinePerKey(sum)
         | 'Write biggerThan10 results to text file' >> beam.io.WriteToText('biggerThan10')
         )

        (smallerThan10
         | 'Calculate sum for smallerThan10' >> beam.CombinePerKey(sum)
         | 'Write smallerThan10 results to text file' >> beam.io.WriteToText('smallerThan10')
         )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
