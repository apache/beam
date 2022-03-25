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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery read jobs configured using
 * {@link BigQuerySchemaTransformReadConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@Internal
@Experimental(Kind.SCHEMAS)
public class BigQuerySchemaTransformReadProvider
    extends TypedSchemaTransformProvider<BigQuerySchemaTransformReadConfiguration> {

  private static final String API = "bigquery";
  private static final String VERSION = "v2";
  private static final String TAG = "ToRows";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<BigQuerySchemaTransformReadConfiguration> configurationClass() {
    return BigQuerySchemaTransformReadConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(BigQuerySchemaTransformReadConfiguration configuration) {
    return new BigQuerySchemaTransformRead(configuration);
  }

  /**
   * An implementation of {@link SchemaTransform} for BigQuery read jobs configured using {@link
   * BigQuerySchemaTransformReadConfiguration}.
   */
  static class BigQuerySchemaTransformRead implements SchemaTransform {
    private final BigQuerySchemaTransformReadConfiguration configuration;

    BigQuerySchemaTransformRead(BigQuerySchemaTransformReadConfiguration configuration) {
      this.configuration = configuration;
    }

    BigQuerySchemaTransformReadConfiguration getConfiguration() {
      return configuration;
    }

    /** Implements {@link SchemaTransform} buildTransform method. */
    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new BigQuerySchemaTransformReadTransform(configuration);
    }
  }

  /**
   * An implementation of {@link PTransform} for BigQuery read jobs configured using {@link
   * BigQuerySchemaTransformReadConfiguration}.
   */
  static class BigQuerySchemaTransformReadTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {
    private final BigQuerySchemaTransformReadConfiguration configuration;

    BigQuerySchemaTransformReadTransform(BigQuerySchemaTransformReadConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (!input.getAll().isEmpty()) {
        throw new IllegalArgumentException("PCollectionRowTuple input is expected to be empty");
      }
      PCollection<TableRow> tableRowPCollection =
          input.getPipeline().apply(configuration.toTypedRead());
      Schema schema = tableRowPCollection.getSchema();
      PCollection<Row> rowPCollection =
          tableRowPCollection.apply(
              MapElements.into(TypeDescriptor.of(Row.class))
                  .via((tableRow) -> BigQueryUtils.toBeamRow(schema, tableRow)));
      return PCollectionRowTuple.of(TAG, rowPCollection);
    }
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return String.format("%s:%s", API, VERSION);
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
    return Collections.singletonList(TAG);
  }
}
