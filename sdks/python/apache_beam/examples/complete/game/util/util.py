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

"""Utility module for common function and classes in the gaming examples"""

from __future__ import absolute_import

import csv
import logging
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions


def str2timestamp(s, fmt='%Y-%m-%d-%H-%M'):
  """Converts a string into a unix timestamp."""
  dt = datetime.strptime(s, fmt)
  epoch = datetime.utcfromtimestamp(0)
  return (dt - epoch).total_seconds()


def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S'):
  """Converts a unix timestamp into a formatted string."""
  return datetime.fromtimestamp(t).strftime(fmt)


class ParseGameEventFn(beam.DoFn):
  """Parses the raw game event info into a Python dictionary.

  Each event line has the following format:
    username,teamname,score,timestamp_in_ms,readable_time

  e.g.:
    user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224

  The human-readable time string is not used here.
  """
  def __init__(self):
    super(ParseGameEventFn, self).__init__()
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, elem):
    try:
      row = list(csv.reader([elem]))[0]
      yield {
          'user': row[0],
          'team': row[1],
          'score': int(row[2]),
          'timestamp': int(row[3]) / 1000.0,
      }
    except:  # pylint: disable=bare-except
      # Log and count parse errors
      self.num_parse_errors.inc()
      logging.error('Parse error on "%s"', elem)


class ExtractAndSumScore(beam.PTransform):
  """A transform to extract key/score information and sum the scores.
  The constructor argument `field` determines whether 'team' or 'user' info is
  extracted.
  """
  def __init__(self, field):
    super(ExtractAndSumScore, self).__init__()
    self.field = field

  def expand(self, pcoll):
    return (pcoll
            | beam.Map(lambda elem: (elem[self.field], elem['score']))
            | beam.CombinePerKey(sum))


class TeamScoresDict(beam.DoFn):
  """Formats the data into a dictionary of BigQuery columns with their values

  Receives a (team, score) pair, extracts the window start timestamp, and
  formats everything together into a dictionary. The dictionary is in the format
  {'bigquery_column': value}
  """
  def process(self, team_score, window=beam.DoFn.WindowParam):
    team, score = team_score
    start = timestamp2str(int(window.start))
    yield {
        'team': team,
        'total_score': score,
        'window_start': start,
        'processing_time': timestamp2str(int(time.time()))
    }


class WriteToBigQuery(beam.PTransform):
  """Generate, format, and write BigQuery table row information."""
  def __init__(self, table_name, dataset, schema):
    """Initializes the transform.
    Args:
      table_name: Name of the BigQuery table to use.
      dataset: Name of the dataset to use.
      schema: Dictionary in the format {'column_name': 'bigquery_type'}
    """
    super(WriteToBigQuery, self).__init__()
    self.table_name = table_name
    self.dataset = dataset
    self.schema = schema
    self.create_disposition = beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    self.write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND

  def get_bigquery_schema(self):
    """Build the output table schema."""
    return ', '.join(
        '%s:%s' % (col, self.schema[col]) for col in self.schema)

  def get_table(self, pipeline):
    """Utility to construct an output table reference."""
    project = pipeline.options.view_as(GoogleCloudOptions).project
    return '%s:%s.%s' % (project, self.dataset, self.table_name)

  def expand(self, pcoll):
    table = self.get_table(pcoll.pipeline)
    return (
        pcoll
        | 'ConvertToRow' >> beam.Map(
            lambda elem: {col: elem[col] for col in self.schema})
        | beam.io.Write(beam.io.BigQuerySink(
            table,
            schema=self.get_bigquery_schema(),
            create_disposition=self.create_disposition,
            write_disposition=self.write_disposition)))
