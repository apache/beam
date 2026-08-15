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

import static org.apache.beam.sdk.io.aws2.kinesis.KinesisReadSchemaTransformProvider.KinesisReadSchemaTransformConfiguration;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;

/** A {@link SchemaTransformProvider} for reading from Amazon Kinesis. */
@AutoService(SchemaTransformProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class KinesisReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<KinesisReadSchemaTransformConfiguration> {

  public static final String OUTPUT_TAG = "output";

  static final Schema OUTPUT_SCHEMA =
      Schema.builder()
          .addByteArrayField("data")
          .addStringField("stream_name")
          .addStringField("partition_key")
          .addStringField("sequence_number")
          .addStringField("shard_id")
          .addDateTimeField("approximate_arrival_timestamp")
          .build();

  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class KinesisReadSchemaTransformConfiguration implements Serializable {
    public static Builder builder() {
      return new AutoValue_KinesisReadSchemaTransformProvider_KinesisReadSchemaTransformConfiguration
          .Builder();
    }

    @SchemaFieldDescription("Kinesis stream name to read from.")
    public abstract String getStreamName();

    @SchemaFieldDescription("AWS access key id.")
    public abstract String getAwsAccessKey();

    @SchemaFieldDescription("AWS secret access key.")
    public abstract String getAwsSecretKey();

    @SchemaFieldDescription("AWS region, for example us-east-1.")
    public abstract String getRegion();

    @SchemaFieldDescription("Optional custom Kinesis service endpoint URI.")
    public abstract @Nullable String getServiceEndpoint();

    @SchemaFieldDescription(
        "Whether to verify TLS certificates. Defaults to true. Never disable in production.")
    public abstract @Nullable Boolean getVerifyCertificate();

    @SchemaFieldDescription(
        "Maximum number of records to read. When set, the resulting PCollection is bounded.")
    public abstract @Nullable Long getMaxNumRecords();

    @SchemaFieldDescription(
        "Maximum read time in milliseconds. When set, the resulting PCollection is bounded.")
    public abstract @Nullable Long getMaxReadTime();

    @SchemaFieldDescription(
        "Where to start reading in the stream: LATEST, TRIM_HORIZON, or AT_TIMESTAMP.")
    public abstract @Nullable String getInitialPositionInStream();

    @SchemaFieldDescription(
        "Epoch millis timestamp used when initial_position_in_stream is AT_TIMESTAMP.")
    public abstract @Nullable Long getInitialTimestampInStream();

    @SchemaFieldDescription("Max records returned by a single GetRecords call (1-10000).")
    public abstract @Nullable Long getRequestRecordsLimit();

    @SchemaFieldDescription(
        "Threshold duration in milliseconds after which a shard is considered up to date.")
    public abstract @Nullable Long getUpToDateThreshold();

    @SchemaFieldDescription("Maximum number of records to hold in memory per shard.")
    public abstract @Nullable Long getMaxCapacityPerShard();

    @SchemaFieldDescription("Watermark policy: ARRIVAL_TIME or PROCESSING_TIME.")
    public abstract @Nullable String getWatermarkPolicy();

    @SchemaFieldDescription(
        "Idle duration threshold in milliseconds for ARRIVAL_TIME watermark policy.")
    public abstract @Nullable Long getWatermarkIdleDurationThreshold();

    @SchemaFieldDescription("Fixed delay between GetRecords calls in milliseconds.")
    public abstract @Nullable Long getRateLimit();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setStreamName(String streamName);

      public abstract Builder setAwsAccessKey(String awsAccessKey);

      public abstract Builder setAwsSecretKey(String awsSecretKey);

      public abstract Builder setRegion(String region);

      public abstract Builder setServiceEndpoint(String serviceEndpoint);

      public abstract Builder setVerifyCertificate(Boolean verifyCertificate);

      public abstract Builder setMaxNumRecords(Long maxNumRecords);

      public abstract Builder setMaxReadTime(Long maxReadTime);

      public abstract Builder setInitialPositionInStream(String initialPositionInStream);

      public abstract Builder setInitialTimestampInStream(Long initialTimestampInStream);

      public abstract Builder setRequestRecordsLimit(Long requestRecordsLimit);

      public abstract Builder setUpToDateThreshold(Long upToDateThreshold);

      public abstract Builder setMaxCapacityPerShard(Long maxCapacityPerShard);

      public abstract Builder setWatermarkPolicy(String watermarkPolicy);

      public abstract Builder setWatermarkIdleDurationThreshold(
          Long watermarkIdleDurationThreshold);

      public abstract Builder setRateLimit(Long rateLimit);

      public abstract KinesisReadSchemaTransformConfiguration build();
    }
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:kinesis_read:v1";
  }

  @Override
  public String description() {
    return "Reads records from an Amazon Kinesis stream and outputs Beam Rows with "
        + "`data` (bytes) plus stream metadata fields.";
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG);
  }

  @Override
  protected SchemaTransform from(KinesisReadSchemaTransformConfiguration configuration) {
    return new KinesisReadSchemaTransform(configuration);
  }

  static class KinesisReadSchemaTransform extends SchemaTransform {
    private final KinesisReadSchemaTransformConfiguration config;

    KinesisReadSchemaTransform(KinesisReadSchemaTransformConfiguration configuration) {
      this.config = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Preconditions.checkState(
          input.getAll().isEmpty(),
          "Expected zero input PCollections for this source, but found: %s",
          input.getAll().keySet());

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

      KinesisIO.Read readTransform =
          KinesisIO.read()
              .withStreamName(config.getStreamName())
              .withClientConfiguration(
                  ClientConfiguration.builder()
                      .credentialsProvider(provider)
                      .region(Region.of(config.getRegion()))
                      .endpoint(endpoint)
                      .skipCertificateVerification(!verifyCertificate)
                      .build());

      if (config.getMaxNumRecords() != null) {
        readTransform = readTransform.withMaxNumRecords(config.getMaxNumRecords());
      }
      if (config.getMaxReadTime() != null) {
        readTransform = readTransform.withMaxReadTime(Duration.millis(config.getMaxReadTime()));
      }
      if (config.getInitialPositionInStream() != null) {
        readTransform =
            readTransform.withInitialPositionInStream(
                InitialPositionInStream.valueOf(config.getInitialPositionInStream()));
      }
      if (config.getInitialTimestampInStream() != null) {
        readTransform =
            readTransform.withInitialTimestampInStream(
                Instant.ofEpochMilli(config.getInitialTimestampInStream()));
      }
      if (config.getRequestRecordsLimit() != null) {
        readTransform =
            readTransform.withRequestRecordsLimit(config.getRequestRecordsLimit().intValue());
      }
      if (config.getUpToDateThreshold() != null) {
        readTransform =
            readTransform.withUpToDateThreshold(Duration.millis(config.getUpToDateThreshold()));
      }
      if (config.getMaxCapacityPerShard() != null) {
        readTransform =
            readTransform.withMaxCapacityPerShard(config.getMaxCapacityPerShard().intValue());
      }
      if (config.getWatermarkPolicy() != null) {
        switch (config.getWatermarkPolicy()) {
          case "ARRIVAL_TIME":
            readTransform =
                config.getWatermarkIdleDurationThreshold() != null
                    ? readTransform.withArrivalTimeWatermarkPolicy(
                        Duration.millis(config.getWatermarkIdleDurationThreshold()))
                    : readTransform.withArrivalTimeWatermarkPolicy();
            break;
          case "PROCESSING_TIME":
            readTransform = readTransform.withProcessingTimeWatermarkPolicy();
            break;
          default:
            throw new IllegalArgumentException(
                "Unsupported watermark_policy: " + config.getWatermarkPolicy());
        }
      }
      if (config.getRateLimit() != null) {
        readTransform =
            readTransform.withFixedDelayRateLimitPolicy(Duration.millis(config.getRateLimit()));
      }

      PCollection<Row> output =
          input
              .getPipeline()
              .apply(readTransform)
              .apply("KinesisRecordToRow", ParDo.of(new KinesisRecordToRowFn()))
              .setRowSchema(OUTPUT_SCHEMA);

      return PCollectionRowTuple.of(OUTPUT_TAG, output);
    }
  }

  static class KinesisRecordToRowFn extends DoFn<KinesisRecord, Row> {
    @ProcessElement
    public void processElement(@Element KinesisRecord record, OutputReceiver<Row> out) {
      Instant arrival = record.getApproximateArrivalTimestamp();
      out.output(
          Row.withSchema(OUTPUT_SCHEMA)
              .withFieldValue("data", record.getDataAsBytes())
              .withFieldValue("stream_name", record.getStreamName())
              .withFieldValue("partition_key", record.getPartitionKey())
              .withFieldValue("sequence_number", record.getSequenceNumber())
              .withFieldValue("shard_id", record.getShardId())
              .withFieldValue(
                  "approximate_arrival_timestamp",
                  arrival != null ? arrival.toDateTime() : Instant.EPOCH.toDateTime())
              .build());
    }
  }
}
