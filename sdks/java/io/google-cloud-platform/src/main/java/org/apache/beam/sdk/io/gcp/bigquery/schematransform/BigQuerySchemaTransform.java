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

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;

@AutoValue
public abstract class BigQuerySchemaTransform implements SchemaTransform {

  public static BigQuerySchemaTransform of(BigQuerySchemaIOConfiguration configuration) {
    return builderOf(configuration)
        .build();
  }

  public static Builder builderOf(BigQuerySchemaIOConfiguration configuration) {
    return new AutoValue_BigQuerySchemaTransform.Builder()
        .setConfiguration(configuration);
  }

  public abstract BigQuerySchemaIOConfiguration getConfiguration();

  @Nullable
  public abstract BigQueryServices getBigQueryServices();

  @Override
  public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
    return new PTransform<PCollectionRowTuple, PCollectionRowTuple>() {
      @Override
      public PCollectionRowTuple expand(PCollectionRowTuple input) {
        if (input.getAll().isEmpty()) {

          BigQueryRowReader.Builder builder = BigQueryRowReader.builderOf(getConfiguration());

          if (getBigQueryServices() != null) {
            builder = builder.setBigQueryServices(getBigQueryServices());
          }

          return input.getPipeline().begin().apply(builder.build());
        }

        // TODO: implement !input.getAll().isEmpty() case
        return null;
      }
    };
  }

  @AutoValue.Builder
  public static abstract class Builder {

    public abstract Builder setConfiguration(BigQuerySchemaIOConfiguration value);

    public abstract Builder setBigQueryServices(BigQueryServices value);

    public abstract BigQuerySchemaTransform build();
  }
}
