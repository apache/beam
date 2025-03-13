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
package org.apache.beam.sdk.io.iceberg;

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.util.RowStringInterpolator;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

class PortableIcebergDestinations implements DynamicDestinations {
  private final RowFilter rowFilter;
  private final RowStringInterpolator interpolator;
  private final String fileFormat;

  public PortableIcebergDestinations(
      String destinationTemplate,
      String fileFormat,
      Schema inputSchema,
      @Nullable List<String> fieldsToDrop,
      @Nullable List<String> fieldsToKeep,
      @Nullable String onlyField) {
    interpolator = new RowStringInterpolator(destinationTemplate, inputSchema);
    RowFilter rf = new RowFilter(inputSchema);

    if (fieldsToDrop != null) {
      rf = rf.drop(fieldsToDrop);
    }
    if (fieldsToKeep != null) {
      rf = rf.keep(fieldsToKeep);
    }
    if (onlyField != null) {
      rf = rf.only(onlyField);
    }
    rowFilter = rf;
    this.fileFormat = fileFormat;
  }

  @Override
  public Schema getDataSchema() {
    return rowFilter.outputSchema();
  }

  @Override
  public Row getData(Row element) {
    return rowFilter.filter(element);
  }

  @Override
  public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
    return interpolator.interpolate(element);
  }

  @Override
  public IcebergDestination instantiateDestination(String dest) {
    return IcebergDestination.builder()
        .setTableIdentifier(TableIdentifier.parse(dest))
        .setTableCreateConfig(null)
        .setFileFormat(FileFormat.fromString(fileFormat))
        .build();
  }
}
