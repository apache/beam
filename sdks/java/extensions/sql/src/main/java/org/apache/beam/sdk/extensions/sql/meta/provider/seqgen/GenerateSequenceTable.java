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
package org.apache.beam.sdk.extensions.sql.meta.provider.seqgen;

import java.io.Serializable;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;

class GenerateSequenceTable extends SchemaBaseBeamTable implements Serializable {
  public static final Schema TABLE_SCHEMA =
      Schema.of(Field.of("sequence", FieldType.INT64), Field.of("event_time", FieldType.DATETIME));

  Integer elementsPerSecond = 5;

  GenerateSequenceTable(Table table) {
    super(TABLE_SCHEMA);
    if (table.getProperties().containsKey("elementsPerSecond")) {
      elementsPerSecond = table.getProperties().getInteger("elementsPerSecond");
    }
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(GenerateSequence.from(0).withRate(elementsPerSecond, Duration.standardSeconds(1)))
        .apply(
            MapElements.into(TypeDescriptor.of(Row.class))
                .via(elm -> Row.withSchema(TABLE_SCHEMA).addValues(elm, Instant.now()).build()))
        .setRowSchema(getSchema());
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.createUnboundedTableStatistics((double) elementsPerSecond);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new UnsupportedOperationException("buildIOWriter unsupported!");
  }
}
