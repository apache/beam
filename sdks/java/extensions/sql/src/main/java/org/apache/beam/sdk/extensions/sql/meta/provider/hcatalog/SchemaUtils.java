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
package org.apache.beam.sdk.extensions.sql.meta.provider.hcatalog;

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hive.hcatalog.data.HCatRecord;

/** Utils to convert between HCatalog types and Beam types. */
class SchemaUtils {

  private static final Map<PrimitiveCategory, FieldType> PRIMITIVE_TYPES_MAP =
      ImmutableMap.<PrimitiveCategory, FieldType>builder()
          .put(PrimitiveCategory.BINARY, FieldType.BYTES)
          .put(PrimitiveCategory.BOOLEAN, FieldType.BOOLEAN)
          .put(PrimitiveCategory.BYTE, FieldType.BYTE)
          .put(PrimitiveCategory.CHAR, FieldType.STRING)
          .put(PrimitiveCategory.DATE, FieldType.DATETIME)
          .put(PrimitiveCategory.DECIMAL, FieldType.DECIMAL)
          .put(PrimitiveCategory.DOUBLE, FieldType.DOUBLE)
          .put(PrimitiveCategory.FLOAT, FieldType.FLOAT)
          .put(PrimitiveCategory.INT, FieldType.INT32)
          .put(PrimitiveCategory.LONG, FieldType.INT64)
          .put(PrimitiveCategory.SHORT, FieldType.INT16)
          .put(PrimitiveCategory.STRING, FieldType.STRING)
          .put(PrimitiveCategory.TIMESTAMP, FieldType.DATETIME)
          .put(PrimitiveCategory.VARCHAR, FieldType.STRING)
          .build();

  private static final Map<String, FieldType> PRIMITIVE_SERDE_TYPES_MAP =
      ImmutableMap.<String, FieldType>builder()
          .put(serdeConstants.BINARY_TYPE_NAME, FieldType.BYTES)
          .put(serdeConstants.BOOLEAN_TYPE_NAME, FieldType.BOOLEAN)
          .put(serdeConstants.TINYINT_TYPE_NAME, FieldType.BYTE)
          .put(serdeConstants.CHAR_TYPE_NAME, FieldType.STRING)
          .put(serdeConstants.DATE_TYPE_NAME, FieldType.DATETIME)
          .put(serdeConstants.DATETIME_TYPE_NAME, FieldType.DATETIME)
          .put(serdeConstants.DECIMAL_TYPE_NAME, FieldType.DECIMAL)
          .put(serdeConstants.DOUBLE_TYPE_NAME, FieldType.DOUBLE)
          .put(serdeConstants.FLOAT_TYPE_NAME, FieldType.FLOAT)
          .put(serdeConstants.INT_TYPE_NAME, FieldType.INT32)
          .put(serdeConstants.BIGINT_TYPE_NAME, FieldType.INT64)
          .put(serdeConstants.SMALLINT_TYPE_NAME, FieldType.INT16)
          .put(serdeConstants.STRING_TYPE_NAME, FieldType.STRING)
          .put(serdeConstants.TIMESTAMP_TYPE_NAME, FieldType.DATETIME)
          .put(serdeConstants.VARCHAR_TYPE_NAME, FieldType.STRING)
          .build();

  static Schema toBeamSchema(List<FieldSchema> fields) {
    return fields.stream().map(fieldSchema -> toBeamField(fieldSchema)).collect(toSchema());
  }

  private static Schema.Field toBeamField(FieldSchema field) {
    String name = field.getName();
    if (!PRIMITIVE_SERDE_TYPES_MAP.containsKey(field.getType())) {
      throw new UnsupportedOperationException(
          "The type '" + field.getType() + "' of field '" + name + "' is not supported.");
    }

    FieldType fieldType = PRIMITIVE_SERDE_TYPES_MAP.get(field.getType());
    return Schema.Field.of(name, fieldType);
  }

  /**
   * Conversion from {@link HCatRecord} to {@link Row}.
   *
   * <p>At the moment doesn't perform any validation, just copies the values over. So errors will
   * happen if Java representation of field values doesn't match.
   */
  static PTransform<PCollection<? extends HCatRecord>, PCollection<Row>> toRow(Schema schema) {
    return ParDo.of(
        new DoFn<HCatRecord, Row>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            HCatRecord hCatRecord = c.element();
            c.output(Row.withSchema(schema).addValues(hCatRecord.getAll()).build());
          }
        });
  }

  public static SerializableFunction<Row, HCatRecord> fromRow() {
    return row -> {
      throw new UnsupportedOperationException(
          "Converting from Beam Rows to HCatRecords is unsupported");
    };
  }
}
