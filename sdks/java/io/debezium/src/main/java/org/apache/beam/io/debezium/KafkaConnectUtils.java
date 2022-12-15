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
}
