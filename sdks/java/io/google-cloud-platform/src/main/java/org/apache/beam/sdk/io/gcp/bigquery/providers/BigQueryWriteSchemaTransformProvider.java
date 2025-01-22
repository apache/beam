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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/**
 * A BigQuery Write SchemaTransformProvider that routes to either {@link
 * BigQueryFileLoadsSchemaTransformProvider} or {@link
 * BigQueryStorageWriteApiSchemaTransformProvider}.
 *
 * <p>Internal only. Used by the Managed Transform layer.
 */
@Internal
@AutoService(SchemaTransformProvider.class)
public class BigQueryWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigQueryWriteConfiguration> {
  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.BIGQUERY_WRITE);
  }

  @Override
  protected SchemaTransform from(BigQueryWriteConfiguration configuration) {
    return new BigQueryWriteSchemaTransform(configuration);
  }

  public static class BigQueryWriteSchemaTransform extends SchemaTransform {
    private final BigQueryWriteConfiguration configuration;

    BigQueryWriteSchemaTransform(BigQueryWriteConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (input.getSinglePCollection().isBounded().equals(PCollection.IsBounded.BOUNDED)) {
        return input.apply(new BigQueryFileLoadsSchemaTransformProvider().from(configuration));
      } else { // UNBOUNDED
        return input.apply(
            new BigQueryStorageWriteApiSchemaTransformProvider().from(configuration));
      }
    }

    public Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically
        return SchemaRegistry.createDefault()
            .getToRowFunction(BigQueryWriteConfiguration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
