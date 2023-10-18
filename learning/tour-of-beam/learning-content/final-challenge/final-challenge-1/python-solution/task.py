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
#   name: FinalSolution1
#   description: Final challenge solution 1.
#   multifile: true
#   files:
#     - name: input.csv
#   context_line: 57
#   categories:
#     - Quickstart
#   complexity: ADVANCED
#   tags:
#     - hellobeam


import apache_beam as beam
import logging
import re
from apache_beam.transforms import window, trigger
from apache_beam.transforms.combiners import CountCombineFn


class Transaction:
    def __init__(self, transaction_no, date, product_no, product_name, price, quantity, customer_no, country):
        self.transaction_no = transaction_no
        self.date = date
        self.product_no = product_no
        self.product_name = product_name
        self.price = price
        self.quantity = quantity
        self.customer_no = customer_no
        self.country = country

    def __str__(self):
        return f"Transaction(transaction_no={self.transaction_no}, date='{self.date}', product_no='{self.product_no}', product_name='{self.product_name}', price={self.price}, quantity={self.quantity}, customer_no={self.customer_no}, country='{self.country}')"


class ExtractDataFn(beam.DoFn):
    def process(self, element):
        items = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', element)
        if items[0] != 'TransactionNo':
            yield Transaction(items[0], items[1], items[2], items[3], items[4], items[5], items[6], items[7])


def partitionTransactions(element, num_partitions):
    if float(element.price) >= 10:
        return 0
    else:
        return 1


def run():
    with beam.Pipeline() as pipeline:
      transactions = (pipeline
                        | 'Read from text file' >> beam.io.ReadFromText('input.csv')
                        | 'Extract Data' >> beam.ParDo(ExtractDataFn())
                        )

      windowed_transactions = (transactions
                                 | 'Window' >> beam.WindowInto(window.FixedWindows(30), trigger=trigger.AfterWatermark(
                    early=trigger.AfterProcessingTime(5).has_ontime_pane(), late=trigger.AfterAll()),
                                                               allowed_lateness=30,
                                                               accumulation_mode=trigger.AccumulationMode.DISCARDING))

      partition = (windowed_transactions
                     | 'Filtering' >> beam.Filter(lambda t: int(t.quantity) >= 20)
                     | 'Partition transactions' >> beam.Partition(partitionTransactions, 2))

      biggerThan10 = partition[0]
      smallerThan10 = partition[1]

      (biggerThan10
         | 'Map product_no and price for bigger' >> beam.Map(lambda transaction: (transaction.product_no, float(transaction.price)))
         | 'Calculate sum for price more than 10' >> beam.CombinePerKey(sum)
         | 'Write price more than 10 results to text file' >> beam.io.WriteToText('price_more_than_10', '.txt', shard_name_template=''))

      (smallerThan10
         | 'Map product_no and price for smaller' >> beam.Map(lambda transaction: (transaction.product_no, float(transaction.price)))
         | 'Calculate sum for price less than 10' >> beam.CombinePerKey(sum)
         | 'Write price less than 10 results to text file' >> beam.io.WriteToText('price_less_than_10', '.txt', shard_name_template=''))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
