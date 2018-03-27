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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamIOType;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;

/**
 * {@code BeamBigQueryTable} represent a BigQuery table as a target.
 * This provider does not currently support being a source.
 *
 */
@Experimental
public class BeamBigQueryTable extends BaseBeamTable implements Serializable {
  private String tableSpec;

  public BeamBigQueryTable(Schema beamSchema, String tableSpec) {
    super(beamSchema);
    this.tableSpec = tableSpec;
  }

  @Override
  public BeamIOType getSourceType() {
    return BeamIOType.BOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(Pipeline pipeline) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PTransform<? super PCollection<Row>, POutput> buildIOWriter() {
    return new PTransform<PCollection<Row>, POutput>() {
      @Override
      public WriteResult expand(PCollection<Row> input) {
        return input.apply(BigQueryIO.<Row>write()
          .withSchema(BigQueryUtils.toTableSchema(getSchema()))
          .withFormatFunction(BigQueryUtils.toTableRow())
          .to(tableSpec));
      }
    };
  }

  public String getTableSpec() {
    return tableSpec;
  }
}
