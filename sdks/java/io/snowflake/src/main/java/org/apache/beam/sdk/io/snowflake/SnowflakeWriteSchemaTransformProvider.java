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
package org.apache.beam.sdk.io.snowflake;

import static org.apache.beam.sdk.io.snowflake.SnowflakeSchemaTransformUtils.parseCreateDisposition;
import static org.apache.beam.sdk.io.snowflake.SnowflakeSchemaTransformUtils.parseStreamingLogLevel;
import static org.apache.beam.sdk.io.snowflake.SnowflakeSchemaTransformUtils.parseWriteDisposition;
import static org.apache.beam.sdk.io.snowflake.SnowflakeSchemaTransformUtils.toSnowflakeTableSchema;

import com.google.auto.service.AutoService;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

/** A {@link SchemaTransformProvider} for writing Beam rows to Snowflake. */
@AutoService(SchemaTransformProvider.class)
public class SnowflakeWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<SnowflakeWriteConfiguration> {

  static final String INPUT_TAG = "input";

  public static final String IDENTIFIER = "beam:schematransform:org.apache.beam:snowflake_write:v1";

  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  @Override
  public String description() {
    return "Writes Beam Rows to Snowflake using batch COPY or streaming Snowpipe.";
  }

  @Override
  protected Class<SnowflakeWriteConfiguration> configurationClass() {
    return SnowflakeWriteConfiguration.class;
  }

  @Override
  protected SchemaTransform from(SnowflakeWriteConfiguration configuration) {
    configuration.validate();
    return new SnowflakeWriteSchemaTransform(configuration);
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  private static class SnowflakeWriteSchemaTransform extends SchemaTransform
      implements Serializable {

    private final SnowflakeWriteConfiguration configuration;

    private SnowflakeWriteSchemaTransform(SnowflakeWriteConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rows = input.get(INPUT_TAG);

      SnowflakeIO.DataSourceConfiguration dataSourceConfiguration =
          SnowflakeSchemaTransformUtils.createDataSourceConfiguration(
              configuration.getServerName(),
              configuration.getUsername(),
              configuration.getPassword(),
              configuration.getOauthToken(),
              configuration.getPrivateKey(),
              configuration.getPrivateKeyPassphrase(),
              configuration.getDatabase(),
              configuration.getSchema(),
              configuration.getWarehouse(),
              configuration.getRole());

      SnowflakeIO.Write<Row> write =
          SnowflakeIO.<Row>write()
              .withDataSourceConfiguration(dataSourceConfiguration)
              .withStagingBucketName(configuration.getStagingBucketName())
              .withStorageIntegrationName(configuration.getStorageIntegrationName())
              .withUserDataMapper(row -> row.getValues().toArray());

      boolean streaming = rows.isBounded() == PCollection.IsBounded.UNBOUNDED;

      if (streaming) {
        String snowPipe = configuration.getSnowPipe();

        if (snowPipe == null || snowPipe.isEmpty()) {
          throw new IllegalArgumentException("snowPipe is required for streaming writes.");
        }

        write = write.withSnowPipe(snowPipe);

        Integer flushRowLimit = configuration.getFlushRowLimit();
        if (flushRowLimit != null) {
          write = write.withFlushRowLimit(flushRowLimit);
        }

        Long flushTimeLimitMillis = configuration.getFlushTimeLimitMillis();
        if (flushTimeLimitMillis != null) {
          write = write.withFlushTimeLimit(Duration.millis(flushTimeLimitMillis));
        }

        Integer shardsNumber = configuration.getShardsNumber();
        if (shardsNumber != null) {
          write = write.withShardsNumber(shardsNumber);
        }

        String debugMode = configuration.getDebugMode();
        if (debugMode != null) {
          write = write.withDebugMode(parseStreamingLogLevel(debugMode));
        }
      } else {
        String table = configuration.getTable();

        if (table == null || table.isEmpty()) {
          throw new IllegalArgumentException("table is required for batch writes.");
        }

        write = write.to(table);

        String createDispositionValue = configuration.getCreateDisposition();

        if (createDispositionValue != null) {
          CreateDisposition createDisposition = parseCreateDisposition(createDispositionValue);

          write = write.withCreateDisposition(createDisposition);

          if (createDisposition == CreateDisposition.CREATE_IF_NEEDED) {
            write = write.withTableSchema(toSnowflakeTableSchema(rows.getSchema()));
          }
        }

        String writeDispositionValue = configuration.getWriteDisposition();

        if (writeDispositionValue != null) {
          write = write.withWriteDisposition(parseWriteDisposition(writeDispositionValue));
        }
      }

      String quotationMark = configuration.getQuotationMark();
      if (quotationMark != null) {
        write = write.withQuotationMark(quotationMark);
      }

      rows.apply("WriteToSnowflake", write);

      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }
}
