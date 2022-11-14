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
package org.apache.beam.sdk.io.singlestore;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for SingleStoreDB parallel read jobs
 * configured using {@link SingleStoreSchemaTransformReadWithPartitionsConfiguration}.
 */
public class SingleStoreSchemaTransformReadWithPartitionsProvider
    extends TypedSchemaTransformProvider<
        SingleStoreSchemaTransformReadWithPartitionsConfiguration> {

  private static final String API = "singlestore";
  private static final String OUTPUT_TAG = "OUTPUT";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<SingleStoreSchemaTransformReadWithPartitionsConfiguration> configurationClass() {
    return SingleStoreSchemaTransformReadWithPartitionsConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(
      SingleStoreSchemaTransformReadWithPartitionsConfiguration configuration) {
    return new SingleStoreReadWithPartitionsSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return String.format("%s:read-with-partitions", API);
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
   * An implementation of {@link SchemaTransform} for SingleStoreDB parallel read jobs configured
   * using {@link SingleStoreSchemaTransformReadWithPartitionsConfiguration}.
   */
  private static class SingleStoreReadWithPartitionsSchemaTransform implements SchemaTransform {
    private final SingleStoreSchemaTransformReadWithPartitionsConfiguration configuration;

    SingleStoreReadWithPartitionsSchemaTransform(
        SingleStoreSchemaTransformReadWithPartitionsConfiguration configuration) {
      this.configuration = configuration;
    }

    /** Implements {@link SchemaTransform} buildTransform method. */
    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new PCollectionRowTupleTransform(configuration);
    }
  }

  /**
   * An implementation of {@link PTransform} for SingleStoreDB read jobs configured using {@link
   * SingleStoreSchemaTransformReadWithPartitionsConfiguration}.
   */
  static class PCollectionRowTupleTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {

    private final SingleStoreSchemaTransformReadWithPartitionsConfiguration configuration;

    PCollectionRowTupleTransform(
        SingleStoreSchemaTransformReadWithPartitionsConfiguration configuration) {
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

      SingleStoreIO.ReadWithPartitionsRows read = SingleStoreIO.readWithPartitionsRows();

      if (configuration.getDataSourceConfiguration() != null) {
        read = read.withDataSourceConfiguration(configuration.getDataSourceConfiguration());
      }

      if (configuration.getTable() != null) {
        read = read.withTable(configuration.getTable());
      }

      if (configuration.getQuery() != null) {
        read = read.withQuery(configuration.getQuery());
      }

      if (configuration.getInitialNumReaders() != null) {
        read = read.withInitialNumReaders(configuration.getInitialNumReaders());
      }

      PCollection<Row> rows = input.getPipeline().apply(read);
      Schema schema = rows.getSchema();

      return PCollectionRowTuple.of(OUTPUT_TAG, rows.setRowSchema(schema));
    }
  }
}
