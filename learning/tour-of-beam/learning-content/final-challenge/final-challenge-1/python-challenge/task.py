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
#     multifile: true
#     files:
#       - name: input.csv
#     context_line: 50
#     categories:
#       - Quickstart
#     complexity: ADVANCED
#     tags:
#       - hellobeam

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


def run():
    with beam.Pipeline() as pipeline:
      transactions = (pipeline
                        | 'Read from text file' >> beam.io.ReadFromText('input.csv'))


if __name__ == '__main__':
    run()
