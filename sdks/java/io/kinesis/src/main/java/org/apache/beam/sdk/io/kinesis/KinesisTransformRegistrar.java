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
package org.apache.beam.sdk.io.kinesis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Exposes {@link KinesisIO.Write} and {@link KinesisIO.Read} as an external transform for
 * cross-language usage.
 */
@Experimental(Kind.PORTABILITY)
@AutoService(ExternalTransformRegistrar.class)
public class KinesisTransformRegistrar implements ExternalTransformRegistrar {
  public static final String WRITE_URN = "beam:external:java:kinesis:write:v1";
  public static final String READ_DATA_URN = "beam:external:java:kinesis:read_data:v1";

  @Override
  public Map<String, Class<? extends ExternalTransformBuilder<?, ?, ?>>> knownBuilders() {
    return ImmutableMap.of(WRITE_URN, WriteBuilder.class, READ_DATA_URN, ReadDataBuilder.class);
  }

  private abstract static class CrossLanguageConfiguration {
    String streamName;
    String awsAccessKey;
    String awsSecretKey;
    Regions region;
    @Nullable String serviceEndpoint;
    boolean verifyCertificate;

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
      this.region = Regions.fromName(region);
    }

    public void setServiceEndpoint(@Nullable String serviceEndpoint) {
      this.serviceEndpoint = serviceEndpoint;
    }

    public void setVerifyCertificate(@Nullable Boolean verifyCertificate) {
      this.verifyCertificate = verifyCertificate == null ? true : verifyCertificate;
    }
  }

  public static class WriteBuilder
      implements ExternalTransformBuilder<WriteBuilder.Configuration, PCollection<byte[]>, PDone> {

    public static class Configuration extends CrossLanguageConfiguration {
      private Properties producerProperties;
      private String partitionKey;

      public void setProducerProperties(Iterable<KV<String, String>> producerProperties) {
        if (producerProperties != null) {
          Properties properties = new Properties();
          producerProperties.forEach(kv -> properties.setProperty(kv.getKey(), kv.getValue()));
          this.producerProperties = properties;
        }
      }

      public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
      }
    }

    @Override
    public PTransform<PCollection<byte[]>, PDone> buildExternal(Configuration configuration) {
      KinesisIO.Write writeTransform =
          KinesisIO.write()
              .withStreamName(configuration.streamName)
              .withAWSClientsProvider(
                  configuration.awsAccessKey,
                  configuration.awsSecretKey,
                  configuration.region,
                  configuration.serviceEndpoint,
                  configuration.verifyCertificate)
              .withPartitionKey(configuration.partitionKey);

      if (configuration.producerProperties != null) {
        writeTransform = writeTransform.withProducerProperties(configuration.producerProperties);
      }

      return writeTransform;
    }
  }

  public static class ReadDataBuilder
      implements ExternalTransformBuilder<
          ReadDataBuilder.Configuration, PBegin, PCollection<byte[]>> {

    public static class Configuration extends CrossLanguageConfiguration {
      @Nullable private Long maxNumRecords;
      @Nullable private Duration maxReadTime;
      @Nullable private InitialPositionInStream initialPositionInStream;
      @Nullable private Instant initialTimestampInStream;
      @Nullable private Integer requestRecordsLimit;
      @Nullable private Duration upToDateThreshold;
      @Nullable private Long maxCapacityPerShard;
      @Nullable private WatermarkPolicy watermarkPolicy;
      @Nullable private Duration watermarkIdleDurationThreshold;
      @Nullable private Duration rateLimit;

      public void setMaxNumRecords(@Nullable Long maxNumRecords) {
        this.maxNumRecords = maxNumRecords;
      }

      public void setMaxReadTime(@Nullable Long maxReadTime) {
        if (maxReadTime != null) {
          this.maxReadTime = Duration.standardSeconds(maxReadTime);
        }
      }

      public void setInitialPositionInStream(@Nullable String initialPositionInStream) {
        if (initialPositionInStream != null) {
          this.initialPositionInStream = InitialPositionInStream.valueOf(initialPositionInStream);
        }
      }

      public void setInitialTimestampInStream(@Nullable Long initialTimestampInStream) {
        if (initialTimestampInStream != null) {
          this.initialTimestampInStream = Instant.ofEpochSecond(initialTimestampInStream);
        }
      }

      public void setRequestRecordsLimit(@Nullable Long requestRecordsLimit) {
        if (requestRecordsLimit != null) {
          this.requestRecordsLimit = requestRecordsLimit.intValue();
        }
      }

      public void setUpToDateThreshold(@Nullable Long upToDateThreshold) {
        if (upToDateThreshold != null) {
          this.upToDateThreshold = Duration.standardSeconds(upToDateThreshold);
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
          this.watermarkIdleDurationThreshold =
              Duration.standardSeconds(watermarkIdleDurationThreshold);
        }
      }

      public void setRateLimit(@Nullable Long rateLimit) {
        if (rateLimit != null) {
          this.rateLimit = Duration.standardSeconds(rateLimit);
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
      KinesisIO.Read<byte[]> readTransform =
          KinesisIO.readData()
              .withStreamName(configuration.streamName)
              .withAWSClientsProvider(
                  configuration.awsAccessKey,
                  configuration.awsSecretKey,
                  configuration.region,
                  configuration.serviceEndpoint,
                  configuration.verifyCertificate);

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
      return readTransform;
    }
  }
}
