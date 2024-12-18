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

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for BigQuery write jobs configured
 * using {@link org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryWriteConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Internal
@AutoService(SchemaTransformProvider.class)
public class BigQueryFileLoadsSchemaTransformProvider
    extends TypedSchemaTransformProvider<BigQueryWriteConfiguration> {

  static final String INPUT_TAG = "input";

  @Override
  protected SchemaTransform from(BigQueryWriteConfiguration configuration) {
    return new BigQueryFileLoadsSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:bigquery_fileloads:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  public static class BigQueryFileLoadsSchemaTransform extends SchemaTransform {
    /** An instance of {@link BigQueryServices} used for testing. */
    private BigQueryServices testBigQueryServices = null;

    private final BigQueryWriteConfiguration configuration;

    BigQueryFileLoadsSchemaTransform(BigQueryWriteConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rowPCollection = input.getSinglePCollection();
      BigQueryIO.Write<Row> write =
          toWrite(rowPCollection.getSchema(), input.getPipeline().getOptions());
      rowPCollection.apply(write);

      return PCollectionRowTuple.empty(input.getPipeline());
    }

    BigQueryIO.Write<Row> toWrite(Schema schema, PipelineOptions options) {
      PortableBigQueryDestinations dynamicDestinations =
          new PortableBigQueryDestinations(schema, configuration);
      BigQueryIO.Write<Row> write =
          BigQueryIO.<Row>write()
              .to(dynamicDestinations)
              .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
              // TODO(https://github.com/apache/beam/issues/33074) BatchLoad's
              // createTempFilePrefixView() doesn't pick up the pipeline option
              .withCustomGcsTempLocation(
                  ValueProvider.StaticValueProvider.of(options.getTempLocation()))
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              // Use Avro format for better performance. Don't change this unless it's for a good reason.
              .withAvroFormatFunction(dynamicDestinations.getAvroFilterFormatFunction(false));

      if (!Strings.isNullOrEmpty(configuration.getCreateDisposition())) {
        CreateDisposition createDisposition =
            CreateDisposition.valueOf(configuration.getCreateDisposition().toUpperCase());
        write = write.withCreateDisposition(createDisposition);
      }
      if (!Strings.isNullOrEmpty(configuration.getWriteDisposition())) {
        WriteDisposition writeDisposition =
            WriteDisposition.valueOf(configuration.getWriteDisposition().toUpperCase());
        write = write.withWriteDisposition(writeDisposition);
      }
      if (!Strings.isNullOrEmpty(configuration.getKmsKey())) {
        write = write.withKmsKey(configuration.getKmsKey());
      }
      if (testBigQueryServices != null) {
        write = write.withTestServices(testBigQueryServices);
      }

      return write;
    }

    /** Setter for testing using {@link BigQueryServices}. */
    @VisibleForTesting
    void setTestBigQueryServices(BigQueryServices testBigQueryServices) {
      this.testBigQueryServices = testBigQueryServices;
    }
  }
}
