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

import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.KEY;

import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * Bigtable reference: <a
 * href=https://cloud.google.com/bigtable/docs/reference/data/rpc/google.bigtable.v2></a>.
 *
 * <p>Requires a mapping which column family corresponds to which column
 *
 * <p>Bigtable {@link com.google.bigtable.v2.Row} is mapped to Beam {@link Row} in the following
 * way:
 *
 * <p>row: key, columnFamily[] -> BEAM_ROW
 *
 * <p>columnFamily: familyName, column[] -> not mapped directly
 *
 * <p>column: columnQualifier, cell[] -> most recent cell
 *
 * <p>cell: value, timestampMicros, labels -> VALUE
 *
 * <p>Mapped Beam {@link Row}:
 *
 * <p>BEAM_ROW: ROW<key STRING, [columnQualifier VALUE]+>
 *
 * <p>VALUE: Beam {@link Schema} type except for ARRAY, DECIMAL, ITERABLE, MAP, ROW
 */
public class BigtableRowToBeamRowFlat
    extends PTransform<PCollection<com.google.bigtable.v2.Row>, PCollection<Row>> {

  private final Schema schema;
  private final Map<String, Set<String>> columnsMapping;

  public BigtableRowToBeamRowFlat(Schema schema, Map<String, Set<String>> columnsMapping) {
    this.schema = schema;
    this.columnsMapping = columnsMapping;
  }

  @Override
  public PCollection<Row> expand(PCollection<com.google.bigtable.v2.Row> input) {
    return input.apply(
        MapElements.via(new BigtableRowToBeamRowFlat.ToBeamRowFn(schema, columnsMapping)));
  }

  private static class ToBeamRowFn extends BigtableRowToBeamRowFn {
    private final Map<String, Set<String>> columnsMapping;

    public ToBeamRowFn(Schema schema, Map<String, Set<String>> columnsMapping) {
      super(schema);
      this.columnsMapping = columnsMapping;
    }

    @Override
    public Row apply(com.google.bigtable.v2.Row bigtableRow) {
      Row.FieldValueBuilder rowBuilder =
          Row.withSchema(schema).withFieldValue(KEY, bigtableRow.getKey().toStringUtf8());

      bigtableRow.getFamiliesList().stream()
          .filter(family -> columnsMapping.containsKey(family.getName()))
          .forEach(family -> setFamily(rowBuilder, family));
      return rowBuilder.build();
    }

    private void setFamily(Row.FieldValueBuilder rowBuilder, Family family) {
      Set<String> columns = columnsMapping.get(family.getName());
      if (columns == null) {
        throw new NullPointerException("Null column list at family " + family.getName());
      } else {
        family.getColumnsList().stream()
            .filter(column -> columns.contains(column.getQualifier().toStringUtf8()))
            .forEach(column -> setColumn(rowBuilder, column));
      }
    }

    private void setColumn(Row.FieldValueBuilder rowBuilder, Column column) {
      String columnName = column.getQualifier().toStringUtf8();
      Schema.FieldType type = schema.getField(columnName).getType();
      rowBuilder.withFieldValue(columnName, getCellValue(getLastCell(column.getCellsList()), type));
    }
  }
}
