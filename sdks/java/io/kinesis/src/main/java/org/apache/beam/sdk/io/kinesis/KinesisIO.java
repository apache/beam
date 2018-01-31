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

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for reading from
 * <a href="https://aws.amazon.com/kinesis/">Kinesis</a> streams.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *     .withStreamName("streamName")
 *     .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *     .withAWSClientsProvider("AWS_KEY", _"AWS_SECRET", STREAM_REGION)
 *  .apply( ... ) // other transformations
 * }</pre>
 *
 * <p>As you can see you need to provide 3 things:
 * <ul>
 *   <li>name of the stream you're going to read</li>
 *   <li>position in the stream where reading should start. There are two options:
 *   <ul>
 *     <li>{@link InitialPositionInStream#LATEST} - reading will begin from end of the stream</li>
 *     <li>{@link InitialPositionInStream#TRIM_HORIZON} - reading will begin at
 *        the very beginning of the stream</li>
 *   </ul></li>
 *   <li>data used to initialize {@link AmazonKinesis} and {@link AmazonCloudWatch} clients:
 *   <ul>
 *     <li>credentials (aws key, aws secret)</li>
 *    <li>region where the stream is located</li>
 *   </ul></li>
 * </ul>
 *
 * <p>In case when you want to set up {@link AmazonKinesis} or {@link AmazonCloudWatch} client by
 * your own (for example if you're using more sophisticated authorization methods like Amazon
 * STS, etc.) you can do it by implementing {@link AWSClientsProvider} class:
 *
 * <pre>{@code
 * public class MyCustomKinesisClientProvider implements AWSClientsProvider {
 *   {@literal @}Override
 *   public AmazonKinesis getKinesisClient() {
 *     // set up your client here
 *   }
 *
 *   public AmazonCloudWatch getCloudWatchClient() {
 *     // set up your client here
 *   }
 *
 * }
 * }</pre>
 *
 * <p>Usage is pretty straightforward:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *    .withStreamName("streamName")
 *    .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *    .withAWSClientsProvider(new MyCustomKinesisClientProvider())
 *  .apply( ... ) // other transformations
 * }</pre>
 *
 * <p>There’s also possibility to start reading using arbitrary point in time -
 * in this case you need to provide {@link Instant} object:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *     .withStreamName("streamName")
 *     .withInitialTimestampInStream(instant)
 *     .withAWSClientsProvider(new MyCustomKinesisClientProvider())
 *  .apply( ... ) // other transformations
 * }</pre>
 *
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public final class KinesisIO {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisIO.class);

  /** Returns a new {@link Read} transform for reading from Kinesis. */
  public static Read read() {
    return new AutoValue_KinesisIO_Read.Builder()
        .setMaxNumRecords(Long.MAX_VALUE)
        .setUpToDateThreshold(Duration.ZERO)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KinesisRecord>> {

    @Nullable
    abstract String getStreamName();

    @Nullable
    abstract StartingPoint getInitialPosition();

    @Nullable
    abstract AWSClientsProvider getAWSClientsProvider();

    abstract long getMaxNumRecords();

    @Nullable
    abstract Duration getMaxReadTime();

    abstract Duration getUpToDateThreshold();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setStreamName(String streamName);

      abstract Builder setInitialPosition(StartingPoint startingPoint);

      abstract Builder setAWSClientsProvider(AWSClientsProvider clientProvider);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Builder setUpToDateThreshold(Duration upToDateThreshold);

      abstract Read build();
    }

    /**
     * Specify reading from streamName.
     */
    public Read withStreamName(String streamName) {
      return toBuilder().setStreamName(streamName).build();
    }

    /**
     * Specify reading from some initial position in stream.
     */
    public Read withInitialPositionInStream(InitialPositionInStream initialPosition) {
      return toBuilder()
          .setInitialPosition(new StartingPoint(initialPosition))
          .build();
    }

    /**
     * Specify reading beginning at given {@link Instant}.
     * This {@link Instant} must be in the past, i.e. before {@link Instant#now()}.
     */
    public Read withInitialTimestampInStream(Instant initialTimestamp) {
      return toBuilder()
          .setInitialPosition(new StartingPoint(initialTimestamp))
          .build();
    }

    /**
     * Allows to specify custom {@link AWSClientsProvider}.
     * {@link AWSClientsProvider} provides {@link AmazonKinesis} and {@link AmazonCloudWatch}
     * instances which are later used for communication with Kinesis.
     * You should use this method if {@link Read#withAWSClientsProvider(String, String, Regions)}
     * does not suit your needs.
     */
    public Read withAWSClientsProvider(AWSClientsProvider awsClientsProvider) {
      return toBuilder().setAWSClientsProvider(awsClientsProvider).build();
    }

    /**
     * Specify credential details and region to be used to read from Kinesis. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * Read#withAWSClientsProvider(AWSClientsProvider)}.
     */
    public Read withAWSClientsProvider(String awsAccessKey, String awsSecretKey, Regions region) {
      return withAWSClientsProvider(awsAccessKey, awsSecretKey, region, null);
    }

    /**
     * Specify credential details and region to be used to read from Kinesis. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * Read#withAWSClientsProvider(AWSClientsProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host. This is useful to execute
     * the tests with a kinesis service emulator.
     */
    public Read withAWSClientsProvider(
        String awsAccessKey, String awsSecretKey, Regions region, String serviceEndpoint) {
      return withAWSClientsProvider(
          new BasicKinesisProvider(awsAccessKey, awsSecretKey, region, serviceEndpoint));
    }

    /** Specifies to read at most a given number of records. */
    public Read withMaxNumRecords(long maxNumRecords) {
      checkArgument(
          maxNumRecords > 0, "maxNumRecords must be positive, but was: %s", maxNumRecords);
      return toBuilder().setMaxNumRecords(maxNumRecords).build();
    }

    /** Specifies to read records during {@code maxReadTime}. */
    public Read withMaxReadTime(Duration maxReadTime) {
      checkArgument(maxReadTime != null, "maxReadTime can not be null");
      return toBuilder().setMaxReadTime(maxReadTime).build();
    }

    /**
     * Specifies how late records consumed by this source can be to still be considered on time.
     * When this limit is exceeded the actual backlog size will be evaluated and the runner might
     * decide to scale the amount of resources allocated to the pipeline in order to
     * speed up ingestion.
     */
    public Read withUpToDateThreshold(Duration upToDateThreshold) {
      checkArgument(upToDateThreshold != null, "upToDateThreshold can not be null");
      return toBuilder().setUpToDateThreshold(upToDateThreshold).build();
    }

    @Override
    public PCollection<KinesisRecord> expand(PBegin input) {
      checkArgument(
          streamExists(getAWSClientsProvider().getKinesisClient(), getStreamName()),
          "Stream %s does not exist",
          getStreamName());

      Unbounded<KinesisRecord> unbounded =
          org.apache.beam.sdk.io.Read.from(
              new KinesisSource(
                  getAWSClientsProvider(),
                  getStreamName(),
                  getInitialPosition(),
                  getUpToDateThreshold()));

      PTransform<PBegin, PCollection<KinesisRecord>> transform = unbounded;

      if (getMaxNumRecords() < Long.MAX_VALUE || getMaxReadTime() != null) {
        transform =
            unbounded.withMaxReadTime(getMaxReadTime()).withMaxNumRecords(getMaxNumRecords());
      }

      return input.apply(transform);
    }

    private static final class BasicKinesisProvider implements AWSClientsProvider {
      private final String accessKey;
      private final String secretKey;
      private final Regions region;
      @Nullable private final String serviceEndpoint;

      private BasicKinesisProvider(
          String accessKey, String secretKey, Regions region, @Nullable String serviceEndpoint) {
        checkArgument(accessKey != null, "accessKey can not be null");
        checkArgument(secretKey != null, "secretKey can not be null");
        checkArgument(region != null, "region can not be null");
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.region = region;
        this.serviceEndpoint = serviceEndpoint;
      }

      private AWSCredentialsProvider getCredentialsProvider() {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
      }

      @Override
      public AmazonKinesis getKinesisClient() {
        AmazonKinesisClientBuilder clientBuilder =
            AmazonKinesisClientBuilder.standard().withCredentials(getCredentialsProvider());
        if (serviceEndpoint == null) {
          clientBuilder.withRegion(region);
        } else {
          clientBuilder.withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region.getName()));
        }
        return clientBuilder.build();
      }

      @Override
      public AmazonCloudWatch getCloudWatchClient() {
        AmazonCloudWatchClientBuilder clientBuilder =
            AmazonCloudWatchClientBuilder.standard().withCredentials(getCredentialsProvider());
        if (serviceEndpoint == null) {
          clientBuilder.withRegion(region);
        } else {
          clientBuilder.withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region.getName()));
        }
        return clientBuilder.build();
      }
    }
  }

  private static boolean streamExists(AmazonKinesis client, String streamName) {
    try {
      DescribeStreamResult describeStreamResult = client.describeStream(streamName);
      return (describeStreamResult != null
          && describeStreamResult.getSdkHttpMetadata().getHttpStatusCode() == 200);
    } catch (Exception e) {
      LOG.warn("Error checking whether stream {} exists.", streamName, e);
    }
    return false;
  }
}
