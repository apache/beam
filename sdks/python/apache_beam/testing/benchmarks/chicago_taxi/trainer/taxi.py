# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utility and schema methods for the chicago_taxi sample."""
# pytype: skip-file

from tensorflow_transform import coders as tft_coders
from tensorflow_transform.tf_metadata import schema_utils

from google.protobuf import text_format  # type: ignore  # typeshed out of date
from tensorflow.python.lib.io import file_io
from tensorflow_metadata.proto.v0 import schema_pb2

# Categorical features are assumed to each have a maximum value in the dataset.
MAX_CATEGORICAL_FEATURE_VALUES = [24, 31, 12]

CATEGORICAL_FEATURE_KEYS = [
    'trip_start_hour',
    'trip_start_day',
    'trip_start_month',
    'pickup_census_tract',
    'dropoff_census_tract',
    'pickup_community_area',
    'dropoff_community_area'
]

DENSE_FLOAT_FEATURE_KEYS = ['trip_miles', 'fare', 'trip_seconds']

# Number of buckets used by tf.transform for encoding each feature.
FEATURE_BUCKET_COUNT = 10

BUCKET_FEATURE_KEYS = [
    'pickup_latitude',
    'pickup_longitude',
    'dropoff_latitude',
    'dropoff_longitude'
]

# Number of vocabulary terms used for encoding VOCAB_FEATURES by tf.transform
VOCAB_SIZE = 1000

# Count of out-of-vocab buckets in which unrecognized VOCAB_FEATURES are hashed.
OOV_SIZE = 10

VOCAB_FEATURE_KEYS = [
    'payment_type',
    'company',
]

LABEL_KEY = 'tips'
FARE_KEY = 'fare'

CSV_COLUMN_NAMES = [
    'pickup_community_area',
    'fare',
    'trip_start_month',
    'trip_start_hour',
    'trip_start_day',
    'trip_start_timestamp',
    'pickup_latitude',
    'pickup_longitude',
    'dropoff_latitude',
    'dropoff_longitude',
    'trip_miles',
    'pickup_census_tract',
    'dropoff_census_tract',
    'payment_type',
    'company',
    'trip_seconds',
    'dropoff_community_area',
    'tips',
]


def transformed_name(key):
  return key + '_xf'


def transformed_names(keys):
  return [transformed_name(key) for key in keys]


# Tf.Transform considers these features as "raw"
def get_raw_feature_spec(schema):
  return schema_utils.schema_as_feature_spec(schema).feature_spec


def make_proto_coder(schema):
  raw_feature_spec = get_raw_feature_spec(schema)
  raw_schema = schema_utils.schema_from_feature_spec(raw_feature_spec)
  return tft_coders.ExampleProtoCoder(raw_schema)


def make_csv_coder(schema):
  """Return a coder for tf.transform to read csv files."""
  raw_feature_spec = get_raw_feature_spec(schema)
  parsing_schema = schema_utils.schema_from_feature_spec(raw_feature_spec)
  return tft_coders.CsvCoder(CSV_COLUMN_NAMES, parsing_schema)


def clean_raw_data_dict(input_dict, raw_feature_spec):
  """Clean raw data dict."""
  output_dict = {}

  for key in raw_feature_spec:
    if key not in input_dict or not input_dict[key]:
      output_dict[key] = []
    else:
      output_dict[key] = [input_dict[key]]
  return output_dict


def make_sql(table_name, max_rows=None, for_eval=False):
  """Creates the sql command for pulling data from BigQuery.

  Args:
    table_name: BigQuery table name
    max_rows: if set, limits the number of rows pulled from BigQuery
    for_eval: True if this is for evaluation, false otherwise

  Returns:
    sql command as string
  """
  if for_eval:
    # 1/3 of the dataset used for eval
    where_clause = 'WHERE MOD(FARM_FINGERPRINT(unique_key), 3) = 0 ' \
                   'AND pickup_latitude is not null AND pickup_longitude ' \
                   'is not null AND dropoff_latitude is not null ' \
                   'AND dropoff_longitude is not null'
  else:
    # 2/3 of the dataset used for training
    where_clause = 'WHERE MOD(FARM_FINGERPRINT(unique_key), 3) > 0 ' \
                   'AND pickup_latitude is not null AND pickup_longitude ' \
                   'is not null AND dropoff_latitude is not null ' \
                   'AND dropoff_longitude is not null'

  limit_clause = ''
  if max_rows:
    limit_clause = 'LIMIT {max_rows}'.format(max_rows=max_rows)
  return """
  SELECT
      CAST(pickup_community_area AS string) AS pickup_community_area,
      CAST(dropoff_community_area AS string) AS dropoff_community_area,
      CAST(pickup_census_tract AS string) AS pickup_census_tract,
      CAST(dropoff_census_tract AS string) AS dropoff_census_tract,
      fare,
      EXTRACT(MONTH FROM trip_start_timestamp) AS trip_start_month,
      EXTRACT(HOUR FROM trip_start_timestamp) AS trip_start_hour,
      EXTRACT(DAYOFWEEK FROM trip_start_timestamp) AS trip_start_day,
      UNIX_SECONDS(trip_start_timestamp) AS trip_start_timestamp,
      pickup_latitude,
      pickup_longitude,
      dropoff_latitude,
      dropoff_longitude,
      trip_miles,
      payment_type,
      company,
      trip_seconds,
      tips
  FROM `{table_name}`
  {where_clause}
  {limit_clause}
""".format(
      table_name=table_name,
      where_clause=where_clause,
      limit_clause=limit_clause)


def read_schema(path):
  """Reads a schema from the provided location.

  Args:
    path: The location of the file holding a serialized Schema proto.

  Returns:
    An instance of Schema or None if the input argument is None
  """
  result = schema_pb2.Schema()
  contents = file_io.read_file_to_string(path)
  text_format.Parse(contents, result)
  return result
