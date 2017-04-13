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
package org.apache.beam.dsls.sql.planner;

import java.util.Map;

import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.beam.dsls.sql.schema.BaseBeamTable;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordTypeCoder;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowCoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link BeamPipelineCreator} converts a {@link BeamRelNode} tree, into a Beam
 * pipeline.
 *
 */
public class BeamPipelineCreator {
  private Map<String, BaseBeamTable> sourceTables;
  private PCollection<BeamSQLRow> latestStream;

  private PipelineOptions options;

  private Pipeline pipeline;

  private boolean hasPersistent = false;

  public BeamPipelineCreator(Map<String, BaseBeamTable> sourceTables) {
    this.sourceTables = sourceTables;

    options = PipelineOptionsFactory.fromArgs(new String[] {}).withValidation()
        .as(PipelineOptions.class); // FlinkPipelineOptions.class
    options.setJobName("BeamPlanCreator");

    pipeline = Pipeline.create(options);
    CoderRegistry cr = pipeline.getCoderRegistry();
    cr.registerCoder(BeamSQLRow.class, BeamSqlRowCoder.of());
    cr.registerCoder(BeamSQLRecordType.class, BeamSQLRecordTypeCoder.of());
  }

  public PCollection<BeamSQLRow> getLatestStream() {
    return latestStream;
  }

  public void setLatestStream(PCollection<BeamSQLRow> latestStream) {
    this.latestStream = latestStream;
  }

  public Map<String, BaseBeamTable> getSourceTables() {
    return sourceTables;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public boolean isHasPersistent() {
    return hasPersistent;
  }

  public void setHasPersistent(boolean hasPersistent) {
    this.hasPersistent = hasPersistent;
  }

}
