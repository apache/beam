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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;
import com.google.auto.service.AutoService;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Exposes {@link KinesisIO.Write} and {@link KinesisIO.Read} as an external transform for
 * cross-language usage.
 */
@AutoService(ExternalTransformRegistrar.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class KinesisTransformRegistrar implements ExternalTransformRegistrar {
  public static final String WRITE_URN = "beam:transform:org.apache.beam:kinesis_write:v2";
  public static final String READ_DATA_URN = "beam:transform:org.apache.beam:kinesis_read_data:v2";

  @Override
  public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    return ImmutableMap.of(WRITE_URN, new WriteBuilder(), READ_DATA_URN, new ReadDataBuilder());
  }

  private abstract static class CrossLanguageConfiguration {
    String streamName;
    String awsAccessKey;
    String awsSecretKey;
    Region region;
    @Nullable URI serviceEndpoint;

    public void setStreamName(String streamName) {
      this.streamName = streamName;
    }

    public void setAwsAccessKey(String awsAccessKey) {
      this.awsAccessKey = awsAccessKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
      this.awsSecretKey = awsSecretKey;
    }

    public void setRegion(String region) {
      this.region = Region.of(region);
    }

    public void setServiceEndpoint(@Nullable String serviceEndpoint) {
      if (serviceEndpoint != null) {
        this.serviceEndpoint = URI(serviceEndpoint);
      }
    }
  }

  public static class WriteBuilder
      implements ExternalTransformBuilder<WriteBuilder.Configuration, PCollection<byte[]>, KinesisIO.Write.Result> {

    public static class Configuration extends CrossLanguageConfiguration {
      private String partitionKey;

      public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
      }
    }

    @Override
    public PTransform<PCollection<byte[]>, KinesisIO.Write.Result> buildExternal(Configuration configuration) {
      AwsBasicCredentials creds = AwsBasicCredentials.create(configuration.awsAccessKey, configuration.awsSecretKey);
      StaticCredentialsProvider provider = StaticCredentialsProvider.create(creds);
      KinesisIO.Write<byte[]> writeTransform =
          KinesisIO.<byte[]>write()
              .withStreamName(configuration.streamName)
              .withClientConfiguration(
                  ClientConfiguration.builder()
                    .credentialsProvider(provider)
                    .region(configuration.region)
                    .endpoint(configuration.serviceEndpoint)
                    .build())
              .withPartitioner(p -> configuration.partitionKey);

      return writeTransform;
    }
  }

  public static class ReadDataBuilder
      implements ExternalTransformBuilder<
          ReadDataBuilder.Configuration, PBegin, PCollection<byte[]>> {

    public static class Configuration extends CrossLanguageConfiguration {
      private @Nullable Long maxNumRecords;
      private @Nullable Duration maxReadTime;
      private @Nullable InitialPositionInStream initialPositionInStream;
      private @Nullable Instant initialTimestampInStream;
      private @Nullable Integer requestRecordsLimit;
      private @Nullable Duration upToDateThreshold;
      private @Nullable Long maxCapacityPerShard;
      private @Nullable WatermarkPolicy watermarkPolicy;
      private @Nullable Duration watermarkIdleDurationThreshold;
      private @Nullable Duration rateLimit;

      public void setMaxNumRecords(@Nullable Long maxNumRecords) {
        this.maxNumRecords = maxNumRecords;
      }

      public void setMaxReadTime(@Nullable Long maxReadTime) {
        if (maxReadTime != null) {
          this.maxReadTime = Duration.millis(maxReadTime);
        }
      }

      public void setInitialPositionInStream(@Nullable String initialPositionInStream) {
        if (initialPositionInStream != null) {
          this.initialPositionInStream = InitialPositionInStream.valueOf(initialPositionInStream);
        }
      }

      public void setInitialTimestampInStream(@Nullable Long initialTimestampInStream) {
        if (initialTimestampInStream != null) {
          this.initialTimestampInStream = Instant.ofEpochMilli(initialTimestampInStream);
        }
      }

      public void setRequestRecordsLimit(@Nullable Long requestRecordsLimit) {
        if (requestRecordsLimit != null) {
          this.requestRecordsLimit = requestRecordsLimit.intValue();
        }
      }

      public void setUpToDateThreshold(@Nullable Long upToDateThreshold) {
        if (upToDateThreshold != null) {
          this.upToDateThreshold = Duration.millis(upToDateThreshold);
        }
      }

      public void setMaxCapacityPerShard(@Nullable Long maxCapacityPerShard) {
        this.maxCapacityPerShard = maxCapacityPerShard;
      }

      public void setWatermarkPolicy(@Nullable String watermarkPolicy) {
        if (watermarkPolicy != null) {
          this.watermarkPolicy = WatermarkPolicy.valueOf(watermarkPolicy);
        }
      }

      public void setWatermarkIdleDurationThreshold(@Nullable Long watermarkIdleDurationThreshold) {
        if (watermarkIdleDurationThreshold != null) {
          this.watermarkIdleDurationThreshold = Duration.millis(watermarkIdleDurationThreshold);
        }
      }

      public void setRateLimit(@Nullable Long rateLimit) {
        if (rateLimit != null) {
          this.rateLimit = Duration.millis(rateLimit);
        }
      }
    }

    private enum WatermarkPolicy {
      ARRIVAL_TIME,
      PROCESSING_TIME
    }

    @Override
    public PTransform<PBegin, PCollection<byte[]>> buildExternal(
        ReadDataBuilder.Configuration configuration) {
      AwsBasicCredentials creds = AwsBasicCredentials.create(configuration.awsAccessKey, configuration.awsSecretKey);
      StaticCredentialsProvider provider = StaticCredentialsProvider.create(creds);
      KinesisIO.Read readTransform =
          KinesisIO.read()
              .withStreamName(configuration.streamName)
              .withClientConfiguration(
                  ClientConfiguration.builder()
                    .credentialsProvider(provider)
                    .region(configuration.region)
                    .endpoint(configuration.serviceEndpoint)
                    .build());

      if (configuration.maxNumRecords != null) {
        readTransform = readTransform.withMaxNumRecords(configuration.maxNumRecords);
      }
      if (configuration.upToDateThreshold != null) {
        readTransform = readTransform.withUpToDateThreshold(configuration.upToDateThreshold);
      }
      if (configuration.maxCapacityPerShard != null) {
        readTransform =
            readTransform.withMaxCapacityPerShard(configuration.maxCapacityPerShard.intValue());
      }
      if (configuration.watermarkPolicy != null) {
        switch (configuration.watermarkPolicy) {
          case ARRIVAL_TIME:
            readTransform =
                configuration.watermarkIdleDurationThreshold != null
                    ? readTransform.withArrivalTimeWatermarkPolicy(
                        configuration.watermarkIdleDurationThreshold)
                    : readTransform.withArrivalTimeWatermarkPolicy();
            break;
          case PROCESSING_TIME:
            readTransform = readTransform.withProcessingTimeWatermarkPolicy();
            break;
          default:
            throw new RuntimeException(
                String.format(
                    "Unsupported watermark policy type: %s", configuration.watermarkPolicy));
        }
      }
      if (configuration.rateLimit != null) {
        readTransform = readTransform.withFixedDelayRateLimitPolicy(configuration.rateLimit);
      }
      if (configuration.maxReadTime != null) {
        readTransform = readTransform.withMaxReadTime(configuration.maxReadTime);
      }
      if (configuration.initialPositionInStream != null) {
        readTransform =
            readTransform.withInitialPositionInStream(configuration.initialPositionInStream);
      }
      if (configuration.requestRecordsLimit != null) {
        readTransform = readTransform.withRequestRecordsLimit(configuration.requestRecordsLimit);
      }
      if (configuration.initialTimestampInStream != null) {
        readTransform =
            readTransform.withInitialTimestampInStream(configuration.initialTimestampInStream);
      }
      // Convert back to bytes to keep consistency with previous verison:
      // https://github.com/apache/beam/blob/5eed396caf9e0065d8ed82edcc236bad5b71ba22/sdks/java/io/kinesis/src/main/java/org/apache/beam/sdk/io/kinesis/KinesisTransformRegistrar.java
      return readTransform.Map(kr -> kr.getDataAsBytes());
    }
  }
}