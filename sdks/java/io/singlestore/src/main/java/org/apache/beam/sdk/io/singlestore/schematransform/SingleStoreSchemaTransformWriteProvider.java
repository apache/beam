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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for SingleStoreDB write jobs configured
 * using {@link SingleStoreSchemaTransformWriteConfiguration}.
 */
@AutoService(SchemaTransformProvider.class)
public class SingleStoreSchemaTransformWriteProvider
    extends TypedSchemaTransformProvider<SingleStoreSchemaTransformWriteConfiguration> {

  private static final String OUTPUT_TAG = "OUTPUT";
  public static final String INPUT_TAG = "INPUT";

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<SingleStoreSchemaTransformWriteConfiguration> configurationClass() {
    return SingleStoreSchemaTransformWriteConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(SingleStoreSchemaTransformWriteConfiguration configuration) {
    return new SingleStoreWriteSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:singlestore_write:v1";
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
   * An implementation of {@link SchemaTransform} for SingleStoreDB write jobs configured using
   * {@link SingleStoreSchemaTransformWriteConfiguration}.
   */
  private static class SingleStoreWriteSchemaTransform extends SchemaTransform {
    private final SingleStoreSchemaTransformWriteConfiguration configuration;

    SingleStoreWriteSchemaTransform(SingleStoreSchemaTransformWriteConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (!input.has(INPUT_TAG)) {
        throw new IllegalArgumentException(
            String.format(
                "%s %s is missing expected tag: %s",
                getClass().getSimpleName(), input.getClass().getSimpleName(), INPUT_TAG));
      }
      SingleStoreIO.DataSourceConfiguration dataSourceConfiguration =
          configuration.getDataSourceConfiguration();
      String table = configuration.getTable();
      Integer batchSize = configuration.getBatchSize();

      SingleStoreIO.Write<Row> write = SingleStoreIO.writeRows();

      if (dataSourceConfiguration != null) {
        write = write.withDataSourceConfiguration(dataSourceConfiguration);
      }

      if (table != null && !table.isEmpty()) {
        write = write.withTable(table);
      }

      if (batchSize != null) {
        write = write.withBatchSize(batchSize);
      }

      PCollection<Integer> res = input.get(INPUT_TAG).apply(write);
      Schema.Builder schemaBuilder = new Schema.Builder();
      schemaBuilder.addField("rowsWritten", Schema.FieldType.INT32);
      Schema schema = schemaBuilder.build();
      return PCollectionRowTuple.of(
          OUTPUT_TAG,
          res.apply(
                  MapElements.into(TypeDescriptor.of(Row.class))
                      .via(
                          (SerializableFunction<Integer, Row>)
                              a -> {
                                Row.Builder rowBuilder = Row.withSchema(schema);
                                rowBuilder.addValue(a);
                                return rowBuilder.build();
                              }))
              .setRowSchema(schema));
    }
  }
}
