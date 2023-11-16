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
package org.apache.beam.sdk.io.singlestore.schematransform;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.singlestore.SingleStoreIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for SingleStoreDB read jobs configured
 * using {@link SingleStoreSchemaTransformReadConfiguration}.
 */
@AutoService(SchemaTransformProvider.class)
public class SingleStoreSchemaTransformReadProvider
    extends TypedSchemaTransformProvider<SingleStoreSchemaTransformReadConfiguration> {
  private static final String OUTPUT_TAG = "OUTPUT";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<SingleStoreSchemaTransformReadConfiguration> configurationClass() {
    return SingleStoreSchemaTransformReadConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(SingleStoreSchemaTransformReadConfiguration configuration) {
    return new SingleStoreReadSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:singlestore_read:v1";
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} inputCollectionNames method. Since
   * no input is expected, this returns an empty list.
   */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. Since
   * a single output is expected, this returns a list with a single name.
   */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  /**
   * An implementation of {@link SchemaTransform} for SingleStoreDB read jobs configured using
   * {@link SingleStoreSchemaTransformReadConfiguration}.
   */
  private static class SingleStoreReadSchemaTransform extends SchemaTransform {
    private final SingleStoreSchemaTransformReadConfiguration configuration;

    SingleStoreReadSchemaTransform(SingleStoreSchemaTransformReadConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (!input.getAll().isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "%s %s input is expected to be empty",
                input.getClass().getSimpleName(), getClass().getSimpleName()));
      }
      SingleStoreIO.DataSourceConfiguration dataSourceConfiguration =
          configuration.getDataSourceConfiguration();
      String table = configuration.getTable();
      String query = configuration.getQuery();
      Boolean outputParallelization = configuration.getOutputParallelization();
      Boolean withPartitions = configuration.getWithPartitions();

      Preconditions.checkArgument(
          !(outputParallelization != null && withPartitions != null && withPartitions),
          "outputParallelization parameter is not supported for partitioned read");

      if (withPartitions != null && withPartitions) {
        SingleStoreIO.ReadWithPartitions<Row> readWithPartitions =
            SingleStoreIO.readWithPartitionsRows();

        if (dataSourceConfiguration != null) {
          readWithPartitions =
              readWithPartitions.withDataSourceConfiguration(dataSourceConfiguration);
        }

        if (table != null && !table.isEmpty()) {
          readWithPartitions = readWithPartitions.withTable(table);
        }

        if (query != null && !query.isEmpty()) {
          readWithPartitions = readWithPartitions.withQuery(query);
        }

        PCollection<Row> rows = input.getPipeline().apply(readWithPartitions);
        Schema schema = rows.getSchema();

        return PCollectionRowTuple.of(OUTPUT_TAG, rows.setRowSchema(schema));
      } else {
        SingleStoreIO.Read<Row> read = SingleStoreIO.readRows();

        if (dataSourceConfiguration != null) {
          read = read.withDataSourceConfiguration(dataSourceConfiguration);
        }

        if (table != null && !table.isEmpty()) {
          read = read.withTable(table);
        }

        if (query != null && !query.isEmpty()) {
          read = read.withQuery(query);
        }

        if (outputParallelization != null) {
          read = read.withOutputParallelization(outputParallelization);
        }

        PCollection<Row> rows = input.getPipeline().apply(read);
        Schema schema = rows.getSchema();

        return PCollectionRowTuple.of(OUTPUT_TAG, rows.setRowSchema(schema));
      }
    }
  }
}
