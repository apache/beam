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
package org.apache.beam.sdk.io.gcp.bigquery.schematransform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.schematransform.BigQuerySchemaIOConfiguration.JobType;
import org.apache.beam.sdk.schemas.io.InvalidConfigurationException;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

@AutoValue
public abstract class BigQueryRowReader extends PTransform<PBegin, PCollectionRowTuple> {

  public static Builder builderOf(BigQuerySchemaIOConfiguration configuration) {
    return new AutoValue_BigQueryRowReader.Builder().setConfiguration(configuration);
  }

  public abstract BigQuerySchemaIOConfiguration getConfiguration();

  private BigQueryIO.TypedRead<TableRow> typedRead() {
    JobType jobType = JobType.valueOf(getConfiguration().getJobType());
    switch (jobType) {
      case QUERY:
        return getConfiguration().toQueryTypedRead();

      case EXTRACT:
        return getConfiguration().toExtractTypedRead();

      default:
        throw new InvalidConfigurationException(
            String.format("invalid job type for BigQueryIO read, got: %s", jobType)
        );
    }
  }

  private String getTag() {
    String jobType = getConfiguration().getJobType().toLowerCase();
    return String.format("%s:%s", BigQuerySchemaIOConfiguration.IDENTIFIER, jobType);
  }

  @Override
  public PCollectionRowTuple expand(PBegin input) {
    PCollection<TableRow> tableRowPCollection = input.apply(typedRead());
    PCollection<Row> rowPCollection = tableRowPCollection
        .apply(ParDo.of(new TableRowToBeamRowFn(tableRowPCollection.getSchema())));
    return PCollectionRowTuple.of(getTag(), rowPCollection);
  }

  @AutoValue.Builder
  public static abstract class Builder {

    public abstract Builder setConfiguration(BigQuerySchemaIOConfiguration value);

    public abstract BigQueryRowReader build();
  }
}
