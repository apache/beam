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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.sdk.io.aws2.kinesis.KinesisWriteSchemaTransformProvider.KinesisWriteSchemaTransformConfiguration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

/** A {@link SchemaTransformProvider} for writing to Amazon Kinesis. */
@AutoService(SchemaTransformProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class KinesisWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<KinesisWriteSchemaTransformConfiguration> {

  public static final String INPUT_TAG = "input";

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class KinesisWriteSchemaTransformConfiguration implements Serializable {
    public static Builder builder() {
      return new AutoValue_KinesisWriteSchemaTransformProvider_KinesisWriteSchemaTransformConfiguration
          .Builder();
    }

    @SchemaFieldDescription("Kinesis stream name to write to.")
    public abstract String getStreamName();

    @SchemaFieldDescription("AWS access key id.")
    public abstract String getAwsAccessKey();

    @SchemaFieldDescription("AWS secret access key.")
    public abstract String getAwsSecretKey();

    @SchemaFieldDescription("AWS region, for example us-east-1.")
    public abstract String getRegion();

    @SchemaFieldDescription(
        "Default partition key used when the input row does not contain a partition_key field.")
    public abstract String getPartitionKey();

    @SchemaFieldDescription("Optional custom Kinesis service endpoint URI.")
    public abstract @Nullable String getServiceEndpoint();

    @SchemaFieldDescription(
        "Whether to verify TLS certificates. Defaults to true. Never disable in production.")
    public abstract @Nullable Boolean getVerifyCertificate();

    @SchemaFieldDescription("Enable KPL-compatible record aggregation.")
    public abstract @Nullable Boolean getAggregationEnabled();

    @SchemaFieldDescription("Max aggregated record size in bytes when aggregation is enabled.")
    public abstract @Nullable Long getAggregationMaxBytes();

    @SchemaFieldDescription(
        "Max time in milliseconds to buffer records when aggregation is enabled.")
    public abstract @Nullable Long getAggregationMaxBufferedTime();

    @SchemaFieldDescription("Shard map refresh interval in minutes when aggregation is enabled.")
    public abstract @Nullable Long getAggregationShardRefreshInterval();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setStreamName(String streamName);

      public abstract Builder setAwsAccessKey(String awsAccessKey);

      public abstract Builder setAwsSecretKey(String awsSecretKey);

      public abstract Builder setRegion(String region);

      public abstract Builder setPartitionKey(String partitionKey);

      public abstract Builder setServiceEndpoint(String serviceEndpoint);

      public abstract Builder setVerifyCertificate(Boolean verifyCertificate);

      public abstract Builder setAggregationEnabled(Boolean aggregationEnabled);

      public abstract Builder setAggregationMaxBytes(Long aggregationMaxBytes);

      public abstract Builder setAggregationMaxBufferedTime(Long aggregationMaxBufferedTime);

      public abstract Builder setAggregationShardRefreshInterval(
          Long aggregationShardRefreshInterval);

      public abstract KinesisWriteSchemaTransformConfiguration build();
    }
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:kinesis_write:v1";
  }

  @Override
  public String description() {
    return "Writes Beam Rows to an Amazon Kinesis stream. Each input row must include a "
        + "`data` (bytes) field, or a `payload` string/bytes field. Optional per-row "
        + "`partition_key` overrides the configured default partition key.";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  protected SchemaTransform from(KinesisWriteSchemaTransformConfiguration configuration) {
    return new KinesisWriteSchemaTransform(configuration);
  }

  static class KinesisWriteSchemaTransform extends SchemaTransform {
    private final KinesisWriteSchemaTransformConfiguration config;

    KinesisWriteSchemaTransform(KinesisWriteSchemaTransformConfiguration configuration) {
      this.config = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> inputRows = input.getSinglePCollection();
      Schema schema = inputRows.getSchema();

      final int dataFieldIndex = resolveDataFieldIndex(schema);
      final boolean dataIsString = isStringField(schema, dataFieldIndex);
      final Integer partitionKeyIndex =
          schema.hasField("partition_key") ? schema.indexOf("partition_key") : null;

      AwsBasicCredentials creds =
          AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey());
      StaticCredentialsProvider provider = StaticCredentialsProvider.create(creds);

      @Nullable URI endpoint = null;
      if (config.getServiceEndpoint() != null) {
        try {
          endpoint = new URI(config.getServiceEndpoint());
        } catch (URISyntaxException ex) {
          throw new IllegalArgumentException(
              String.format("Service endpoint must be a URI, got: %s", config.getServiceEndpoint()),
              ex);
        }
      }

      boolean verifyCertificate =
          config.getVerifyCertificate() == null || config.getVerifyCertificate();

      SerializableFunction<KV<String, byte[]>, byte[]> serializer = KV::getValue;
      KinesisIO.Write<KV<String, byte[]>> writeTransform =
          KinesisIO.<KV<String, byte[]>>write()
              .withStreamName(config.getStreamName())
              .withClientConfiguration(
                  ClientConfiguration.builder()
                      .credentialsProvider(provider)
                      .region(Region.of(config.getRegion()))
                      .endpoint(endpoint)
                      .skipCertificateVerification(!verifyCertificate)
                      .build())
              .withPartitioner(KV::getKey)
              .withSerializer(serializer);

      if (Boolean.TRUE.equals(config.getAggregationEnabled())) {
        KinesisIO.RecordAggregation.Builder aggregation = KinesisIO.RecordAggregation.builder();
        if (config.getAggregationMaxBytes() != null) {
          aggregation.maxBytes(config.getAggregationMaxBytes().intValue());
        }
        if (config.getAggregationMaxBufferedTime() != null) {
          aggregation.maxBufferedTime(Duration.millis(config.getAggregationMaxBufferedTime()));
        }
        if (config.getAggregationShardRefreshInterval() != null) {
          aggregation.shardRefreshInterval(
              Duration.standardMinutes(config.getAggregationShardRefreshInterval()));
        }
        writeTransform = writeTransform.withRecordAggregation(aggregation.build());
      } else {
        writeTransform = writeTransform.withRecordAggregationDisabled();
      }

      final String defaultPartitionKey = config.getPartitionKey();
      inputRows
          .apply(
              "ExtractKinesisRecord",
              ParDo.of(
                  new DoFn<Row, KV<String, byte[]>>() {
                    @ProcessElement
                    public void processElement(
                        @Element Row row, OutputReceiver<KV<String, byte[]>> out) {
                      byte[] data;
                      if (dataIsString) {
                        String payload =
                            Preconditions.checkStateNotNull(row.getString(dataFieldIndex));
                        data = payload.getBytes(StandardCharsets.UTF_8);
                      } else {
                        data = Preconditions.checkStateNotNull(row.getBytes(dataFieldIndex));
                      }
                      String partitionKey = defaultPartitionKey;
                      if (partitionKeyIndex != null) {
                        String rowKey = row.getString(partitionKeyIndex);
                        if (rowKey != null && !rowKey.isEmpty()) {
                          partitionKey = rowKey;
                        }
                      }
                      out.output(KV.of(partitionKey, data));
                    }
                  }))
          .apply(writeTransform);

      return PCollectionRowTuple.empty(inputRows.getPipeline());
    }

    private static int resolveDataFieldIndex(Schema schema) {
      if (schema.hasField("data")
          && schema.getField("data").getType().equals(Schema.FieldType.BYTES)) {
        return schema.indexOf("data");
      }
      if (schema.hasField("payload")
          && (schema.getField("payload").getType().equals(Schema.FieldType.BYTES)
              || schema.getField("payload").getType().equals(Schema.FieldType.STRING))) {
        return schema.indexOf("payload");
      }
      if (schema.getFieldCount() == 1
          && (schema.getField(0).getType().equals(Schema.FieldType.BYTES)
              || schema.getField(0).getType().equals(Schema.FieldType.STRING))) {
        return 0;
      }
      throw new IllegalArgumentException(
          "Expected input schema with a 'data' (BYTES) field, a 'payload' "
              + "(BYTES/STRING) field, or a single bytes/string field, but got: "
              + schema);
    }

    private static boolean isStringField(Schema schema, int index) {
      return schema.getField(index).getType().equals(Schema.FieldType.STRING);
    }
  }
}
