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

import com.google.bigtable.v2.Cell;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;

class BigtableRowToBeamRowFn extends SimpleFunction<com.google.bigtable.v2.Row, Row> {

  protected final Schema schema;

  private final CellValueParser valueParser = new CellValueParser();

  public BigtableRowToBeamRowFn(Schema schema) {
    this.schema = schema;
  }

  protected Cell getLastCell(List<Cell> cells) {
    return cells.stream()
        .max(Comparator.comparingLong(Cell::getTimestampMicros))
        .orElseThrow(() -> new RuntimeException("Couldn't retrieve the most recent cell value"));
  }

  protected Object getCellValue(Cell cell, Schema.FieldType type) {
    return valueParser.getCellValue(cell, type);
  }
}
