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

import json
import typing

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


def word_count_jinja_parameter_data():
  params = {
      "readFromTextTransform": {
          "path": "gs://dataflow-samples/shakespeare/kinglear.txt"
      },
      "mapToFieldsSplitConfig": {
          "language": "python", "fields": {
              "value": "1"
          }
      },
      "explodeTransform": {
          "fields": "word"
      },
      "combineTransform": {
          "group_by": "word", "combine": {
              "value": "sum"
          }
      },
      "mapToFieldsCountConfig": {
          "language": "python",
          "fields": {
              "output": "word + \" - \" + str(value)"
          }
      },
      "writeToTextTransform": {
          "path": "gs://apache-beam-testing-derrickaw/wordCounts/"
      }
  }
  return json.dumps(params)


def word_count_jinja_template_data(test_name: str) -> list[str]:
  if test_name == 'test_wordCountInclude_yaml':
    return [
        'apache_beam/yaml/examples/transforms/jinja/'
        'include/submodules/readFromTextTransform.yaml',
        'apache_beam/yaml/examples/transforms/jinja/'
        'include/submodules/mapToFieldsSplitConfig.yaml',
        'apache_beam/yaml/examples/transforms/jinja/'
        'include/submodules/explodeTransform.yaml',
        'apache_beam/yaml/examples/transforms/jinja/'
        'include/submodules/combineTransform.yaml',
        'apache_beam/yaml/examples/transforms/jinja/'
        'include/submodules/mapToFieldsCountConfig.yaml',
        'apache_beam/yaml/examples/transforms/jinja/'
        'include/submodules/writeToTextTransform.yaml'
    ]
  elif test_name == 'test_wordCountImport_yaml':
    return [
        'apache_beam/yaml/examples/transforms/jinja/'
        'import/macros/wordCountMacros.yaml'
    ]
  return []


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


def youtube_comments_csv():
  return '\n'.join([
      'video_id,comment_text,likes,replies',
      'XpVt6Z1Gjjo,I AM HAPPY,1,1',
      'XpVt6Z1Gjjo,I AM SAD,1,1',
      'XpVt6Z1Gjjo,§ÁĐ,1,1'
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


def pubsub_taxi_ride_events_data():
  """
  Provides a list of PubsubMessage objects for testing taxi ride events.
  """
  return [
      PubsubMessage(
          data=b"{\"ride_id\": \"1\", \"longitude\": 11.0, \"latitude\": -11.0,"
          b"\"passenger_count\": 1, \"meter_reading\": 100.0, \"timestamp\": "
          b"\"2025-01-01T00:29:00.00000-04:00\", \"ride_status\": \"pickup\"}",
          attributes={}),
      PubsubMessage(
          data=b"{\"ride_id\": \"2\", \"longitude\": 22.0, \"latitude\": -22.0,"
          b"\"passenger_count\": 2, \"meter_reading\": 100.0, \"timestamp\": "
          b"\"2025-01-01T00:30:00.00000-04:00\", \"ride_status\": \"pickup\"}",
          attributes={}),
      PubsubMessage(
          data=b"{\"ride_id\": \"1\", \"longitude\": 13.0, \"latitude\": -13.0,"
          b"\"passenger_count\": 1, \"meter_reading\": 100.0, \"timestamp\": "
          b"\"2025-01-01T00:31:00.00000-04:00\", \"ride_status\": \"enroute\"}",
          attributes={}),
      PubsubMessage(
          data=b"{\"ride_id\": \"2\", \"longitude\": 24.0, \"latitude\": -24.0,"
          b"\"passenger_count\": 2, \"meter_reading\": 100.0, \"timestamp\": "
          b"\"2025-01-01T00:32:00.00000-04:00\", \"ride_status\": \"enroute\"}",
          attributes={}),
      PubsubMessage(
          data=b"{\"ride_id\": \"3\", \"longitude\": 33.0, \"latitude\": -33.0,"
          b"\"passenger_count\": 3, \"meter_reading\": 100.0, \"timestamp\": "
          b"\"2025-01-01T00:35:00.00000-04:00\", \"ride_status\": \"enroute\"}",
          attributes={}),
      PubsubMessage(
          data=b"{\"ride_id\": \"4\", \"longitude\": 44.0, \"latitude\": -44.0,"
          b"\"passenger_count\": 4, \"meter_reading\": 100.0, \"timestamp\": "
          b"\"2025-01-01T00:35:00.00000-04:00\", \"ride_status\": \"dropoff\"}",
          attributes={}),
      PubsubMessage(
          data=b"{\"ride_id\": \"1\", \"longitude\": 15.0, \"latitude\": -15.0,"
          b"\"passenger_count\": 1, \"meter_reading\": 100.0, \"timestamp\": "
          b"\"2025-01-01T00:33:00.00000-04:00\", \"ride_status\": \"dropoff\"}",
          attributes={}),
      PubsubMessage(
          data=b"{\"ride_id\": \"2\", \"longitude\": 26.0, \"latitude\": -26.0,"
          b"\"passenger_count\": 2, \"meter_reading\": 100.0, \"timestamp\": "
          b"\"2025-01-01T00:34:00.00000-04:00\", \"ride_status\": \"dropoff\"}",
          attributes={}),
  ]


def kafka_messages_data():
  """
  Provides a list of Kafka messages for testing.
  """
  return [data.encode('utf-8') for data in text_data().split('\n')]


class TaxiRideEventSchema(typing.NamedTuple):
  ride_id: str
  longitude: float
  latitude: float
  passenger_count: int
  meter_reading: float
  timestamp: str
  ride_status: str


def system_logs_csv():
  return '\n'.join([
      'LineId,Date,Time,Level,Process,Component,Content',
      '1,2024-10-01,12:00:00,INFO,Main,ComponentA,System started successfully',
      '2,2024-10-01,12:00:05,WARN,Main,ComponentA,Memory usage is high',
      '3,2024-10-01,12:00:10,ERROR,Main,ComponentA,Task failed due to timeout',
  ])


def system_logs_data():
  csv_data = system_logs_csv()
  lines = csv_data.strip().split('\n')
  headers = lines[0].split(',')
  logs = []
  for row in lines[1:]:
    values = row.split(',')
    log = dict(zip(headers, values))
    log['LineId'] = int(log['LineId'])
    logs.append(log)

  return logs


def embedding_data():
  return [0.1, 0.2, 0.3, 0.4, 0.5]


def system_logs_embedding_data():
  csv_data = system_logs_csv()
  lines = csv_data.strip().split('\n')
  headers = lines[0].split(',')
  headers.append('embedding')
  logs = []
  for row in lines[1:]:
    values = row.split(',')
    values.append(embedding_data())
    log = dict(zip(headers, values))
    log['LineId'] = int(log['LineId'])
    logs.append(log)

  return logs
