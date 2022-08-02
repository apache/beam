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
package org.apache.beam.sdk.io.gcp.bigtable;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.LABELS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.TIMESTAMP_MICROS;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.VALUE;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Bigtable reference: <a
 * href=https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2></a>.
 *
 * <p>Bigtable {@link com.google.bigtable.v2.Row} is mapped to Beam {@link Row} in the following
 * way:
 *
 * <p>row: key, columnFamily[] -> BEAM_ROW
 *
 * <p>columnFamily: familyName, column[] -> FAMILY
 *
 * <p>column: columnQualifier, cell[] -> most recent cell or ARRAY<cell>
 *
 * <p>cell: value, timestampMicros, labels -> VALUE
 *
 * <p>Mapped Beam {@link Row}:
 *
 * <p>BEAM_ROW: ROW<key STRING, [familyName FAMILY]+>
 *
 * <p>FAMILY: ROW<[columnName ARRAY<VALUE> or VALUE]+>
 *
 * <p>VALUE:ROW_CELL or {@link Schema} type except for: ARRAY, DECIMAL, ITERABLE, MAP, ROW
 *
 * <p>ROW_CELL: ROW<val VALUE [, timestampMicros INT64] [, labels ARRAY<STRING>]>
 *
 * <p>Note: ARRAY<ROW_CELL> is not supported for now.
 */
public class BigtableRowToBeamRow
    extends PTransform<PCollection<com.google.bigtable.v2.Row>, PCollection<Row>>
    implements Serializable {

  private final Schema schema;

  public BigtableRowToBeamRow(Schema schema) {
    this.schema = schema;
  }

  @Override
  public PCollection<Row> expand(PCollection<com.google.bigtable.v2.Row> input) {
    return input.apply(MapElements.via(new ToBeamRowFn(schema)));
  }

  private static class ToBeamRowFn extends BigtableRowToBeamRowFn {
    ToBeamRowFn(Schema schema) {
      super(schema);
    }

    @Override
    public Row apply(com.google.bigtable.v2.Row bigtableRow) {
      return bigtableRowToBeamRow(bigtableRow);
    }

    private Row cellToRow(Cell cell, Schema cellSchema) {

      Row.FieldValueBuilder rowBuilder =
          Row.withSchema(cellSchema)
              .withFieldValue(VALUE, getCellValue(cell, cellSchema.getField(VALUE).getType()));
      if (cellSchema.hasField(TIMESTAMP_MICROS)) {
        rowBuilder.withFieldValue(TIMESTAMP_MICROS, cell.getTimestampMicros());
      }
      if (cellSchema.hasField(LABELS)) {
        rowBuilder.withFieldValue(LABELS, cell.getLabelsList());
      }
      return rowBuilder.build();
    }

    // Returns Simple type, List<Simple type> or Row
    private Object columnToRow(Column column, Schema schema) {
      String columnName = column.getQualifier().toStringUtf8();
      Schema.FieldType columnType = schema.getField(columnName).getType();
      List<Cell> cells = column.getCellsList();
      switch (columnType.getTypeName()) {
        case ARRAY:
          Schema.FieldType collectionElementType = columnType.getCollectionElementType();
          if (collectionElementType != null) {
            return cells.stream()
                .map(cell -> getCellValue(cell, collectionElementType))
                .collect(toList());
          } else {
            throw new NullPointerException("Null collectionElementType at column " + columnName);
          }
        case ROW:
          @Nullable Schema rowSchema = columnType.getRowSchema();
          if (rowSchema == null) {
            throw new NullPointerException("Null row schema at column " + columnName);
          } else {
            return cellToRow(getLastCell(cells), rowSchema);
          }
        default:
          return getCellValue(getLastCell(cells), columnType);
      }
    }

    private Row familyToRow(Family family, Schema schema) {
      Map<String, Object> columns =
          family.getColumnsList().stream()
              .filter(column -> schema.hasField(column.getQualifier().toStringUtf8()))
              .map(
                  column -> {
                    String columnName = column.getQualifier().toStringUtf8();
                    return KV.of(columnName, columnToRow(column, schema));
                  })
              .collect(
                  Collectors.toMap(
                      KV::getKey,
                      kv -> {
                        Object value = kv.getValue();
                        if (value == null) {
                          throw new NullPointerException("Null value at column " + kv.getKey());
                        } else {
                          return value;
                        }
                      }));
      return Row.withSchema(schema).withFieldValues(columns).build();
    }

    private Row bigtableRowToBeamRow(com.google.bigtable.v2.Row bigtableRow) {
      Row.FieldValueBuilder rowBuilder =
          Row.withSchema(schema).withFieldValue(KEY, bigtableRow.getKey().toStringUtf8());
      bigtableRow.getFamiliesList().stream()
          .filter(family -> schema.hasField(family.getName()))
          .forEach(
              family -> {
                Schema familySchema = schema.getField(family.getName()).getType().getRowSchema();
                if (familySchema == null) {
                  throw new NullPointerException(
                      "Null family schema at family " + family.getName());
                } else {
                  rowBuilder.withFieldValue(family.getName(), familyToRow(family, familySchema));
                }
              });
      return rowBuilder.build();
    }
  }
}
