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

"""Tools used tool work with Avro files in the context of BigQuery.

Classes, constants and functions in this file are experimental and have no
backwards compatibility guarantees.

NOTHING IN THIS FILE HAS BACKWARDS COMPATIBILITY GUARANTEES.
"""

# BigQuery types as listed in
# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
# with aliases (RECORD, BOOLEAN, FLOAT, INTEGER) as defined in
# https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableFieldSchema.html#setType-java.lang.String-
BIG_QUERY_TO_AVRO_TYPES = {
    "STRUCT": "record",
    "RECORD": "record",
    "STRING": "string",
    "BOOL": "boolean",
    "BOOLEAN": "boolean",
    "BYTES": "bytes",
    "FLOAT64": "double",
    "FLOAT": "double",
    "INT64": "long",
    "INTEGER": "long",
    "TIME": {
        "type": "long",
        "logicalType": "time-micros",
    },
    "TIMESTAMP": {
        "type": "long",
        "logicalType": "timestamp-micros",
    },
    "DATE": {
        "type": "int",
        "logicalType": "date",
    },
    "DATETIME": "string",
    "NUMERIC": {
        "type": "bytes",
        "logicalType": "decimal",
        # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric-type
        "precision": 38,
        "scale": 9,
    },
    "GEOGRAPHY": "string",
}


def get_record_schema_from_dict_table_schema(
    schema_name, table_schema, namespace="apache_beam.io.gcp.bigquery"):
  # type: (Text, Dict[Text, Any], Text) -> Dict[Text, Any]

  """Convert a table schema into an Avro schema.

  Args:
    schema_name (Text): The name of the record.
    table_schema (Dict[Text, Any]): A BigQuery table schema in dict form.
    namespace (Text): The namespace of the Avro schema.

  Returns:
    Dict[Text, Any]: The schema as an Avro RecordSchema.
  """
  avro_fields = [
      table_field_to_avro_field(field, ".".join((namespace, schema_name)))
      for field in table_schema["fields"]
  ]

  return {
      "type": "record",
      "name": schema_name,
      "fields": avro_fields,
      "doc": "Translated Avro Schema for {}".format(schema_name),
      "namespace": namespace,
  }


def table_field_to_avro_field(table_field, namespace):
  # type: (Dict[Text, Any], str) -> Dict[Text, Any]

  """Convert a BigQuery field to an avro field.

  Args:
    table_field (Dict[Text, Any]): A BigQuery field in dict form.

  Returns:
    Dict[Text, Any]: An equivalent Avro field in dict form.
  """
  assert "type" in table_field, \
    "Unable to get type for table field {}".format(table_field)
  assert table_field["type"] in BIG_QUERY_TO_AVRO_TYPES, \
    "Unable to map BigQuery field type {} to avro type".format(
      table_field["type"])

  avro_type = BIG_QUERY_TO_AVRO_TYPES[table_field["type"]]

  if avro_type == "record":
    element_type = get_record_schema_from_dict_table_schema(
        table_field["name"],
        table_field,
        namespace=".".join((namespace, table_field["name"])))
  else:
    element_type = avro_type

  field_mode = table_field.get("mode", "NULLABLE")

  if field_mode in (None, "NULLABLE"):
    field_type = ["null", element_type]
  elif field_mode == "REQUIRED":
    field_type = element_type
  elif field_mode == "REPEATED":
    field_type = {"type": "array", "items": element_type}
  else:
    raise ValueError("Unkown BigQuery field mode: {}".format(field_mode))

  avro_field = {"type": field_type, "name": table_field["name"]}

  doc = table_field.get("description")
  if doc:
    avro_field["doc"] = doc

  return avro_field
