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
package org.apache.beam.sdk.extensions.sql.meta.provider.datagen;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * Represents a 'datagen' table within a Beam SQL pipeline. This class extends {@link
 * SchemaBaseBeamTable} to correctly implement the full {@link
 * org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable} interface.
 */
public class DataGeneratorTable extends SchemaBaseBeamTable {
  private final ObjectNode properties;

  public DataGeneratorTable(Schema schema, ObjectNode properties) {
    super(schema);
    this.properties = properties;
  }

  @Override
  public IsBounded isBounded() {
    // The table is bounded if 'number-of-rows' is specified, otherwise it is unbounded.
    return properties.has("number-of-rows") ? IsBounded.BOUNDED : IsBounded.UNBOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin.apply(
        "ReadFromDataGenerator", new DataGeneratorPTransform(getSchema(), properties));
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
    // DataGenerator does not support filter or project push-down.
    // This is a common pattern for IOs that do not support these optimizations.
    return buildIOReader(begin);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("The 'datagen' table type is read-only.");
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    // Safely provide statistics to the query planner.
    // Check for the bounded property first.
    if (properties.has("number-of-rows")) {
      double rowCount = properties.get("number-of-rows").asDouble();
      return BeamTableStatistics.createBoundedTableStatistics(rowCount);
    }
    // Then check for the unbounded property.
    else if (properties.has("rows-per-second")) {
      double rate = properties.get("rows-per-second").asDouble();
      return BeamTableStatistics.createUnboundedTableStatistics(rate);
    }

    // If neither property is present, the table is misconfigured. Return UNKNOWN
    // and let the PTransform's expand() method throw the detailed error.
    return BeamTableStatistics.BOUNDED_UNKNOWN;
  }
}
