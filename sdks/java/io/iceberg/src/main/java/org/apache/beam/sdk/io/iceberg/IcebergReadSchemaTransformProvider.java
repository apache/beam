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

import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.managed.ManagedTransformConstants;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.iceberg.IcebergReadSchemaTransformProvider.Config;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * SchemaTransform implementation for {@link IcebergIO#readRows}. Reads records from Iceberg and
 * outputs a {@link org.apache.beam.sdk.values.PCollection} of Beam {@link
 * org.apache.beam.sdk.values.Row}s.
 */
@AutoService(SchemaTransformProvider.class)
public class IcebergReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<SchemaTransformConfiguration> {
  static final String OUTPUT_TAG = "output";

  @Override
  protected SchemaTransform from(SchemaTransformConfiguration configuration) {
    return new IcebergReadSchemaTransform(configuration);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  public String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.ICEBERG_READ);
  }

  static class IcebergReadSchemaTransform extends SchemaTransform {
    private final SchemaTransformConfiguration configuration;

    IcebergReadSchemaTransform(SchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically and convert field names to snake_case
        return SchemaRegistry.createDefault()
            .getToRowFunction(SchemaTransformConfiguration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> output =
          input
              .getPipeline()
              .apply(
                  IcebergIO.readRows(configuration.getIcebergCatalog())
                      .from(TableIdentifier.parse(configuration.getTable())));

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }
}
