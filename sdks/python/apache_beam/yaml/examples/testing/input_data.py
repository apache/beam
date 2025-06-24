# coding=utf-8
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

from apache_beam.io.gcp.pubsub import PubsubMessage

# This file contains the input data to be requested by the example tests, if
# needed.


def text_data():
  return '\n'.join([
      "Fool\tThou shouldst not have been old till thou hadst",
      "\tbeen wise.",
      "KING LEAR\tNothing will come of nothing: speak again.",
      "\tNever, never, never, never, never!"
  ])


def iceberg_dynamic_destinations_users_data():
  return [{
      'id': 3, 'name': 'Smith', 'email': 'smith@example.com', 'zip': 'NY'
  },
          {
              'id': 4,
              'name': 'Beamberg',
              'email': 'beamberg@example.com',
              'zip': 'NY'
          }]


def products_csv():
  return '\n'.join([
      'transaction_id,product_name,category,price',
      'T0012,Headphones,Electronics,59.99',
      'T5034,Leather Jacket,Apparel,109.99',
      'T0024,Aluminum Mug,Kitchen,29.99',
      'T0104,Headphones,Electronics,59.99',
      'T0302,Monitor,Electronics,249.99'
  ])


def spanner_orders_data():
  return [{
      'order_id': 1,
      'customer_id': 1001,
      'product_id': 2001,
      'order_date': '24-03-24',
      'order_amount': 150,
  },
          {
              'order_id': 2,
              'customer_id': 1002,
              'product_id': 2002,
              'order_date': '19-04-24',
              'order_amount': 90,
          },
          {
              'order_id': 3,
              'customer_id': 1003,
              'product_id': 2003,
              'order_date': '7-05-24',
              'order_amount': 110,
          }]


def spanner_shipments_data():
  return [{
      'shipment_id': 'S1',
      'customer_id': 'C1',
      'shipment_date': '2023-05-01',
      'shipment_cost': 150.0,
      'customer_name': 'Alice',
      'customer_email': 'alice@example.com'
  },
          {
              'shipment_id': 'S2',
              'customer_id': 'C2',
              'shipment_date': '2023-06-12',
              'shipment_cost': 300.0,
              'customer_name': 'Bob',
              'customer_email': 'bob@example.com'
          },
          {
              'shipment_id': 'S3',
              'customer_id': 'C1',
              'shipment_date': '2023-05-10',
              'shipment_cost': 20.0,
              'customer_name': 'Alice',
              'customer_email': 'alice@example.com'
          },
          {
              'shipment_id': 'S4',
              'customer_id': 'C4',
              'shipment_date': '2024-07-01',
              'shipment_cost': 150.0,
              'customer_name': 'Derek',
              'customer_email': 'derek@example.com'
          },
          {
              'shipment_id': 'S5',
              'customer_id': 'C5',
              'shipment_date': '2023-05-09',
              'shipment_cost': 300.0,
              'customer_name': 'Erin',
              'customer_email': 'erin@example.com'
          },
          {
              'shipment_id': 'S6',
              'customer_id': 'C4',
              'shipment_date': '2024-07-02',
              'shipment_cost': 150.0,
              'customer_name': 'Derek',
              'customer_email': 'derek@example.com'
          }]


def shipments_data():
  return [{
      'shipment_id': 'S1',
      'customer_id': 'C1',
      'shipment_date': '2023-05-01',
      'shipment_cost': 150.0,
      'customer_name': 'Alice',
      'customer_email': 'alice@example.com'
  },
          {
              'shipment_id': 'S2',
              'customer_id': 'C2',
              'shipment_date': '2023-06-12',
              'shipment_cost': 300.0,
              'customer_name': 'Bob',
              'customer_email': 'bob@example.com'
          },
          {
              'shipment_id': 'S3',
              'customer_id': 'C1',
              'shipment_date': '2023-05-10',
              'shipment_cost': 20.0,
              'customer_name': 'Alice',
              'customer_email': 'alice@example.com'
          },
          {
              'shipment_id': 'S4',
              'customer_id': 'C4',
              'shipment_date': '2024-07-01',
              'shipment_cost': 150.0,
              'customer_name': 'Derek',
              'customer_email': 'derek@example.com'
          },
          {
              'shipment_id': 'S5',
              'customer_id': 'C5',
              'shipment_date': '2023-05-09',
              'shipment_cost': 300.0,
              'customer_name': 'Erin',
              'customer_email': 'erin@example.com'
          },
          {
              'shipment_id': 'S6',
              'customer_id': 'C4',
              'shipment_date': '2024-07-02',
              'shipment_cost': 150.0,
              'customer_name': 'Derek',
              'customer_email': 'derek@example.com'
          }]


def bigtable_data():
  return [{
      'product_id': '1', 'product_name': 'pixel 5', 'product_stock': '2'
  }, {
      'product_id': '2', 'product_name': 'pixel 6', 'product_stock': '4'
  }, {
      'product_id': '3', 'product_name': 'pixel 7', 'product_stock': '20'
  }, {
      'product_id': '4', 'product_name': 'pixel 8', 'product_stock': '10'
  }, {
      'product_id': '5', 'product_name': 'pixel 11', 'product_stock': '3'
  }, {
      'product_id': '6', 'product_name': 'pixel 12', 'product_stock': '7'
  }, {
      'product_id': '7', 'product_name': 'pixel 13', 'product_stock': '8'
  }, {
      'product_id': '8', 'product_name': 'pixel 14', 'product_stock': '3'
  }]


def bigquery_data():
  return [{
      'customer_id': 1001,
      'customer_name': 'Alice',
      'customer_email': 'alice@gmail.com'
  },
          {
              'customer_id': 1002,
              'customer_name': 'Bob',
              'customer_email': 'bob@gmail.com'
          },
          {
              'customer_id': 1003,
              'customer_name': 'Claire',
              'customer_email': 'claire@gmail.com'
          }]


def pubsub_messages_data():
  """
  Provides a list of PubsubMessage objects for testing.
  """
  return [
      PubsubMessage(data=b"{\"label\": \"37a\", \"rank\": 1}", attributes={}),
      PubsubMessage(data=b"{\"label\": \"37b\", \"rank\": 4}", attributes={}),
      PubsubMessage(data=b"{\"label\": \"37c\", \"rank\": 3}", attributes={}),
      PubsubMessage(data=b"{\"label\": \"37d\", \"rank\": 2}", attributes={}),
  ]
