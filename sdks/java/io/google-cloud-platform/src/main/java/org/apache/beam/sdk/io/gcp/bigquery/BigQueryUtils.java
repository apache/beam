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

package org.apache.beam.sdk.io.gcp.bigquery;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.beam.sdk.values.Row.toRow;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Utility methods for BigQuery related operations.
 *
 * <p><b>Example: Writing to BigQuery</b>
 *
 * <pre>{@code
 * PCollection<Row> rows = ...;
 *
 * rows.apply(BigQueryIO.<Row>write()
 *       .withSchema(BigQueryUtils.toTableSchema(rows))
 *       .withFormatFunction(BigQueryUtils.toTableRow())
 *       .to("my-project:my_dataset.my_table"));
 * }</pre>
 */
public class BigQueryUtils {
  private static final Map<TypeName, StandardSQLTypeName> BEAM_TO_BIGQUERY_TYPE_MAPPING =
      ImmutableMap.<TypeName, StandardSQLTypeName>builder()
          .put(TypeName.BYTE, StandardSQLTypeName.INT64)
          .put(TypeName.INT16, StandardSQLTypeName.INT64)
          .put(TypeName.INT32, StandardSQLTypeName.INT64)
          .put(TypeName.INT64, StandardSQLTypeName.INT64)
          .put(TypeName.FLOAT, StandardSQLTypeName.FLOAT64)
          .put(TypeName.DOUBLE, StandardSQLTypeName.FLOAT64)
          .put(TypeName.DECIMAL, StandardSQLTypeName.FLOAT64)
          .put(TypeName.BOOLEAN, StandardSQLTypeName.BOOL)
          .put(TypeName.ARRAY, StandardSQLTypeName.ARRAY)
          .put(TypeName.ROW, StandardSQLTypeName.STRUCT)
          .put(TypeName.DATETIME, StandardSQLTypeName.TIMESTAMP)
          .put(TypeName.STRING, StandardSQLTypeName.STRING)
          .build();

  private static final Map<TypeName, Function<String, Object>> JSON_VALUE_PARSERS =
      ImmutableMap.<TypeName, Function<String, Object>>builder()
          .put(TypeName.BYTE, Byte::valueOf)
          .put(TypeName.INT16, Short::valueOf)
          .put(TypeName.INT32, Integer::valueOf)
          .put(TypeName.INT64, Long::valueOf)
          .put(TypeName.FLOAT, Float::valueOf)
          .put(TypeName.DOUBLE, Double::valueOf)
          .put(TypeName.DECIMAL, BigDecimal::new)
          .put(TypeName.BOOLEAN, Boolean::valueOf)
          .put(TypeName.STRING, str -> str)
          .build();

  private static final Map<String, StandardSQLTypeName> BEAM_TO_BIGQUERY_METADATA_MAPPING =
      ImmutableMap.<String, StandardSQLTypeName>builder()
          .put("DATE", StandardSQLTypeName.DATE)
          .put("TIME", StandardSQLTypeName.TIME)
          .put("TIME_WITH_LOCAL_TZ", StandardSQLTypeName.TIME)
          .put("TS", StandardSQLTypeName.TIMESTAMP)
          .put("TS_WITH_LOCAL_TZ", StandardSQLTypeName.TIMESTAMP)
          .build();

  /**
   * Get the corresponding BigQuery {@link StandardSQLTypeName} for supported Beam {@link
   * FieldType}.
   */
  private static StandardSQLTypeName toStandardSQLTypeName(FieldType fieldType) {
    StandardSQLTypeName sqlType = BEAM_TO_BIGQUERY_TYPE_MAPPING.get(fieldType.getTypeName());

    if (sqlType == StandardSQLTypeName.TIMESTAMP && fieldType.getMetadata() != null) {
      sqlType = BEAM_TO_BIGQUERY_METADATA_MAPPING.get(
          new String(fieldType.getMetadata(), StandardCharsets.UTF_8));
    }

    return sqlType;
  }

  private static List<TableFieldSchema> toTableFieldSchema(Schema schema) {
    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>(schema.getFieldCount());
    for (Field schemaField : schema.getFields()) {
      FieldType type = schemaField.getType();

      TableFieldSchema field = new TableFieldSchema().setName(schemaField.getName());
      if (schemaField.getDescription() != null && !"".equals(schemaField.getDescription())) {
        field.setDescription(schemaField.getDescription());
      }

      if (!schemaField.getNullable()) {
        field.setMode(Mode.REQUIRED.toString());
      }
      if (TypeName.ARRAY == type.getTypeName()) {
        type = type.getCollectionElementType();
        field.setMode(Mode.REPEATED.toString());
      }
      if (TypeName.ROW == type.getTypeName()) {
        Schema subType = type.getRowSchema();
        field.setFields(toTableFieldSchema(subType));
      }
      field.setType(toStandardSQLTypeName(type).toString());

      fields.add(field);
    }
    return fields;
  }

  /** Convert a Beam {@link Schema} to a BigQuery {@link TableSchema}. */
  public static TableSchema toTableSchema(Schema schema) {
    return new TableSchema().setFields(toTableFieldSchema(schema));
  }

  /** Convert a Beam {@link PCollection} to a BigQuery {@link TableSchema}. */
  public static TableSchema toTableSchema(PCollection<Row> rows) {
    RowCoder coder = (RowCoder) rows.getCoder();
    return toTableSchema(coder.getSchema());
  }

  private static final SerializableFunction<Row, TableRow> TO_TABLE_ROW = new ToTableRow();

  /** Convert a Beam {@link Row} to a BigQuery {@link TableRow}. */
  public static SerializableFunction<Row, TableRow> toTableRow() {
    return TO_TABLE_ROW;
  }

  /** Convert a Beam {@link Row} to a BigQuery {@link TableRow}. */
  private static class ToTableRow implements SerializableFunction<Row, TableRow> {
    @Override
    public TableRow apply(Row input) {
      return toTableRow(input);
    }
  }

  public static TableRow toTableRow(Row row) {
    TableRow output = new TableRow();
    for (int i = 0; i < row.getFieldCount(); i++) {
      Object value = row.getValue(i);

      Field schemaField = row.getSchema().getField(i);
      TypeName type = schemaField.getType().getTypeName();

      if (TypeName.ARRAY == type) {
        type = schemaField.getType().getCollectionElementType().getTypeName();
        if (TypeName.ROW == type) {
          List<Row> rows = (List<Row>) value;
          List<TableRow> tableRows = new ArrayList<TableRow>(rows.size());
          for (int j = 0; j < rows.size(); j++) {
            tableRows.add(toTableRow(rows.get(j)));
          }
          value = tableRows;
        }
      } else if (TypeName.ROW == type) {
        value = toTableRow((Row) value);
      }

      output = output.set(schemaField.getName(), value);
    }
    return output;
  }

  /**
   * Tries to parse the JSON {@link TableRow} from BigQuery.
   *
   * <p>Only supports basic types and arrays. Doesn't support date types.
   */
  public static Row toBeamRow(Schema rowSchema, TableSchema bqSchema, TableRow jsonBqRow) {
    List<TableFieldSchema> bqFields = bqSchema.getFields();

    Map<String, Integer> bqFieldIndices =
        IntStream.range(0, bqFields.size())
            .boxed()
            .collect(toMap(i -> bqFields.get(i).getName(), i -> i));

    List<Object> rawJsonValues =
        rowSchema
            .getFields()
            .stream()
            .map(field -> bqFieldIndices.get(field.getName()))
            .map(index -> jsonBqRow.getF().get(index).getV())
            .collect(toList());

    return IntStream.range(0, rowSchema.getFieldCount())
        .boxed()
        .map(index -> toBeamValue(rowSchema.getField(index).getType(), rawJsonValues.get(index)))
        .collect(toRow(rowSchema));
  }

  private static Object toBeamValue(FieldType fieldType, Object jsonBQValue) {
    if (jsonBQValue instanceof String && JSON_VALUE_PARSERS.containsKey(fieldType.getTypeName())) {
      return JSON_VALUE_PARSERS.get((fieldType.getTypeName())).apply((String) jsonBQValue);
    }

    if (jsonBQValue instanceof List) {
      return ((List<Object>) jsonBQValue)
          .stream()
          .map(v -> ((Map<String, Object>) v).get("v"))
          .map(v -> toBeamValue(fieldType.getCollectionElementType(), v))
          .collect(toList());
    }

    throw new UnsupportedOperationException(
        "Converting BigQuery type '"
            + jsonBQValue.getClass()
            + "' to '"
            + fieldType
            + "' is not supported");
  }
}
