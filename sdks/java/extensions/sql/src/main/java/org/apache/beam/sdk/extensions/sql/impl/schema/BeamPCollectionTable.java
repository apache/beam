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
package org.apache.beam.sdk.extensions.sql.impl.schema;

import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * {@code BeamPCollectionTable} converts a {@code PCollection<Row>} as a virtual table, then a
 * downstream query can query directly.
 */
public class BeamPCollectionTable<InputT> extends SchemaBaseBeamTable {
  private transient PCollection<InputT> upstream;

  public BeamPCollectionTable(PCollection<InputT> upstream) {
    super(upstream.getSchema());
    if (!upstream.hasSchema()) {
      throw new IllegalArgumentException("SQL can only run over PCollections that have schemas.");
    }
    this.upstream = upstream;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return upstream.isBounded();
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    assert begin.getPipeline() == upstream.getPipeline();
    return upstream.apply(Convert.toRows());
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    throw new IllegalArgumentException("cannot use [BeamPCollectionTable] as target");
  }

  @Override
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    return BeamTableStatistics.BOUNDED_UNKNOWN;
  }
}
