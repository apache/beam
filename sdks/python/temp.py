"""
**Note:** input and output element types need to be PCollections of Beam :py:class:`apache_beam.pvalue.Row`.
"""

import datetime

import pytz

import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp


users_schema = {
  "fields": [
    {"type": "INTEGER", "name": "id", "mode": "NULLABLE"},
    {"type": "TIMESTAMP", "name": "date", "mode": "NULLABLE"},
  ]
}

with beam.Pipeline() as p:
  p | beam.Create([{"id": 1, "date": Timestamp.from_utc_datetime(datetime.datetime(2000, 9, 15, tzinfo=pytz.UTC))},
                   {"id": 2, "date": Timestamp.from_utc_datetime(datetime.datetime(100, 1, 1, tzinfo=pytz.UTC))}]
                  ) | beam.io.WriteToBigQuery(
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    schema=users_schema,
                    table="google.com:clouddfe:ahmedabualsaud_test.jdbc-repro2",
                    method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
                  )
