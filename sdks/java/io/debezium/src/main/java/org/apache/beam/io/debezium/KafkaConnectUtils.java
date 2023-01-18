/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.io.debezium;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.joda.time.Instant;

public class KafkaConnectUtils {
  public static Schema beamSchemaFromKafkaConnectSchema(
      org.apache.kafka.connect.data.Schema kafkaSchema) {
    assert kafkaSchema.type().equals(org.apache.kafka.connect.data.Schema.Type.STRUCT)
        : "Beam Rows are encoded from Kafka Struct schemas.";
    Schema.Builder beamSchemaBuilder = Schema.builder();

    for (org.apache.kafka.connect.data.Field field : kafkaSchema.fields()) {
      Schema.Field beamField =
          field.schema().isOptional()
              ? Schema.Field.nullable(field.name(), beamSchemaTypeFromKafkaType(field.schema()))
              : Schema.Field.of(field.name(), beamSchemaTypeFromKafkaType(field.schema()));
      if (field.schema().doc() != null) {
        beamField = beamField.withDescription(field.schema().doc());
      }
      beamSchemaBuilder.addField(beamField);
    }
    return beamSchemaBuilder.build();
  }

  public static Schema.FieldType beamSchemaTypeFromKafkaType(
      org.apache.kafka.connect.data.Schema kafkaFieldSchema) {
    switch (kafkaFieldSchema.type()) {
      case STRUCT:
        return Schema.FieldType.row(beamSchemaFromKafkaConnectSchema(kafkaFieldSchema));
      case INT8:
        return Schema.FieldType.BYTE;
      case INT16:
        return Schema.FieldType.INT16;
      case INT32:
        return Schema.FieldType.INT32;
      case INT64:
        return Schema.FieldType.INT64;
      case FLOAT32:
        return Schema.FieldType.FLOAT;
      case FLOAT64:
        return Schema.FieldType.DOUBLE;
      case BOOLEAN:
        return Schema.FieldType.BOOLEAN;
      case STRING:
        return Schema.FieldType.STRING;
      case BYTES:
        return Schema.FieldType.BYTES;
      case ARRAY:
        return Schema.FieldType.array(beamSchemaTypeFromKafkaType(kafkaFieldSchema.valueSchema()));
      case MAP:
        return Schema.FieldType.map(
            beamSchemaTypeFromKafkaType(kafkaFieldSchema.keySchema()),
            beamSchemaTypeFromKafkaType(kafkaFieldSchema.valueSchema()));
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unable to convert Kafka field schema %s to Beam Schema", kafkaFieldSchema));
    }
  }

  public static Instant debeziumRecordInstant(SourceRecord record) {
    if (!record.valueSchema().type().equals(org.apache.kafka.connect.data.Schema.Type.STRUCT)
        || record.valueSchema().field("ts_ms") == null) {
      throw new IllegalArgumentException(
          "Debezium record received is not of the right kind. "
              + String.format(
                  "Should be STRUCT with ts_ms field. Instead it is: %s", record.valueSchema()));
    }
    Struct recordValue = (Struct) record.value();
    return Instant.ofEpochMilli(recordValue.getInt64("ts_ms"));
  }

  public static SourceRecordMapper<Row> beamRowFromSourceRecordFn(final Schema recordSchema) {
    return new SourceRecordMapper<Row>() {
      @Override
      public Row mapSourceRecord(SourceRecord sourceRecord) throws Exception {
        return beamRowFromKafkaStruct((Struct) sourceRecord.value(), recordSchema);
      }

      private Row beamRowFromKafkaStruct(Struct kafkaStruct, Schema beamSchema) {
        Row.Builder rowBuilder = Row.withSchema(beamSchema);
        for (Schema.Field f : beamSchema.getFields()) {
          Object structField = kafkaStruct.getWithoutDefault(f.getName());
          switch (kafkaStruct.schema().field(f.getName()).schema().type()) {
            case ARRAY:
            case MAP:
              // TODO(pabloem): Handle nested structs
              throw new IllegalArgumentException("UNABLE TO CONVERT FIELD " + f);
            case STRUCT:
              Schema fieldSchema = f.getType().getRowSchema();
              if (fieldSchema == null) {
                throw new IllegalArgumentException(
                    String.format(
                        "Improper schema for Beam record: %s has no row schema to build a Row from.",
                        f.getName()));
              }
              if (structField == null) {
                // If the field is null, then we must add a null field to ensure we encode things
                // properly.
                rowBuilder = rowBuilder.addValue(null);
                break;
              }
              rowBuilder =
                  rowBuilder.addValue(beamRowFromKafkaStruct((Struct) structField, fieldSchema));
              break;
            default:
              rowBuilder = rowBuilder.addValue(structField);
              break;
          }
        }
        return rowBuilder.build();
      }
    };
  }
}
