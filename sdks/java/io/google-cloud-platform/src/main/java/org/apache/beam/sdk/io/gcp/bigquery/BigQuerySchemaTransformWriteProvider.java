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
 * {@link BigQuerySchemaTransformWriteConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@Internal
@Experimental(Kind.SCHEMAS)
public class BigQuerySchemaTransformWriteProvider
    extends TypedSchemaTransformProvider<BigQuerySchemaTransformWriteConfiguration> {

  private static final String API = "bigquery";
  private static final String VERSION = "v2";
  private static final String TAG = "FromRows";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<BigQuerySchemaTransformWriteConfiguration> configurationClass() {
    return BigQuerySchemaTransformWriteConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(BigQuerySchemaTransformWriteConfiguration configuration) {
    return new BigQuerySchemaTransformWrite(configuration);
  }

  /**
   * An implementation of {@link SchemaTransform} for BigQuery write jobs configured using {@link
   * BigQuerySchemaTransformWriteConfiguration}.
   */
  static class BigQuerySchemaTransformWrite implements SchemaTransform {
    private final BigQuerySchemaTransformWriteConfiguration configuration;

    BigQuerySchemaTransformWrite(BigQuerySchemaTransformWriteConfiguration configuration) {
      this.configuration = configuration;
    }

    BigQuerySchemaTransformWriteConfiguration getConfiguration() {
      return configuration;
    }

    @Override
    public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildTransform() {
      return new BigQuerySchemaTransformWriteTransform(configuration);
    }
  }

  /**
   * An implementation of {@link PTransform} for BigQuery write jobs configured using {@link
   * BigQuerySchemaTransformWriteConfiguration}.
   */
  static class BigQuerySchemaTransformWriteTransform
      extends PTransform<PCollectionRowTuple, PCollectionRowTuple> {
    private final BigQuerySchemaTransformWriteConfiguration configuration;

    BigQuerySchemaTransformWriteTransform(BigQuerySchemaTransformWriteConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (!input.has(TAG)) {
        throw new IllegalArgumentException(
            String.format("expected PCollection for tag: %s is missing", TAG));
      }
      PCollection<Row> rowPCollection = input.get(TAG);
      Schema schema = rowPCollection.getSchema();
      PCollection<TableRow> tableRowPCollection =
          rowPCollection.apply(
              MapElements.into(TypeDescriptor.of(TableRow.class)).via(BigQueryUtils::toTableRow));
      tableRowPCollection.apply(configuration.toWrite(schema));
      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return String.format("%s:%s", API, VERSION);
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} inputCollectionNames method. Since a
   * single is expected, this returns a list with a single name.
   */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(TAG);
  }

  /**
   * Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. Since
   * no output is expected, this returns an empty list.
   */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }
}
