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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.function.Supplier;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.kinesis.common.InitialPositionInStream;

/**
 * {@link PTransform}s for reading from <a href="https://aws.amazon.com/kinesis/">Kinesis</a>
 * streams.
 *
 * <p>Note that KinesisIO.Write is based on the Kinesis Producer Library which does not yet have an
 * update to be compatible with AWS SDK for Java version 2 so for now the version in {@code
 * org.apache.beam.sdk.io.kinesis} should be used for writing to Kinesis.
 *
 * <h3>Reading from Kinesis</h3>
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
 *
 * <ul>
 *   <li>name of the stream you're going to read
 *   <li>position in the stream where reading should start. There are two options:
 *       <ul>
 *         <li>{@link InitialPositionInStream#LATEST} - reading will begin from end of the stream
 *         <li>{@link InitialPositionInStream#TRIM_HORIZON} - reading will begin at the very
 *             beginning of the stream
 *       </ul>
 *   <li>data used to initialize {@link KinesisClient} and {@link CloudWatchClient} clients:
 *       <ul>
 *         <li>credentials (aws key, aws secret)
 *         <li>region where the stream is located
 *       </ul>
 * </ul>
 *
 * <p>In case when you want to set up {@link KinesisClient} or {@link CloudWatchClient} client by
 * your own (for example if you're using more sophisticated authorization methods like Amazon STS,
 * etc.) you can do it by implementing {@link AWSClientsProvider} class:
 *
 * <pre>{@code
 * public class MyCustomKinesisClientProvider implements AWSClientsProvider {
 *   public KinesisClient getKinesisClient() {
 *     // set up your client here
 *   }
 *
 *   public CloudWatchClient getCloudWatchClient() {
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
 * <p>There’s also possibility to start reading using arbitrary point in time - in this case you
 * need to provide {@link Instant} object:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *     .withStreamName("streamName")
 *     .withInitialTimestampInStream(instant)
 *     .withAWSClientsProvider(new MyCustomKinesisClientProvider())
 *  .apply( ... ) // other transformations
 * }</pre>
 *
 * <p>Kinesis IO uses ArrivalTimeWatermarkPolicy by default. To use Processing time as event time:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *    .withStreamName("streamName")
 *    .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *    .withProcessingTimeWatermarkPolicy())
 * }</pre>
 *
 * <p>It is also possible to specify a custom watermark policy to control watermark computation.
 * Below is an example
 *
 * <pre>{@code
 * // custom policy
 * class MyCustomPolicy implements WatermarkPolicy {
 *     private WatermarkPolicyFactory.CustomWatermarkPolicy customWatermarkPolicy;
 *
 *     MyCustomPolicy() {
 *       this.customWatermarkPolicy = new WatermarkPolicyFactory.CustomWatermarkPolicy(WatermarkParameters.create());
 *     }
 *
 *     public Instant getWatermark() {
 *       return customWatermarkPolicy.getWatermark();
 *     }
 *
 *     public void update(KinesisRecord record) {
 *       customWatermarkPolicy.update(record);
 *     }
 *   }
 *
 * // custom factory
 * class MyCustomPolicyFactory implements WatermarkPolicyFactory {
 *     public WatermarkPolicy createWatermarkPolicy() {
 *       return new MyCustomPolicy();
 *     }
 * }
 *
 * p.apply(KinesisIO.read()
 *    .withStreamName("streamName")
 *    .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *    .withCustomWatermarkPolicy(new MyCustomPolicyFactory())
 * }</pre>
 *
 * <p>By default Kinesis IO will poll the Kinesis getRecords() API as fast as possible which may
 * lead to excessive read throttling. To limit the rate of getRecords() calls you can set a rate
 * limit policy. For example, the default fixed delay policy will limit the rate to one API call per
 * second per shard:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *    .withStreamName("streamName")
 *    .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *    .withFixedDelayRateLimitPolicy())
 * }</pre>
 *
 * <p>You can also use a fixed delay policy with a specified delay interval, for example:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *    .withStreamName("streamName")
 *    .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *    .withFixedDelayRateLimitPolicy(Duration.millis(500))
 * }</pre>
 *
 * <p>If you need to change the polling interval of a Kinesis pipeline at runtime, for example to
 * compensate for adding and removing additional consumers to the stream, then you can supply the
 * delay interval as a function so that you can obtain the current delay interval from some external
 * source:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *    .withStreamName("streamName")
 *    .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *    .withDynamicDelayRateLimitPolicy(() -> Duration.millis(<some delay interval>))
 * }</pre>
 *
 * <p>Finally, you can create a custom rate limit policy that responds to successful read calls
 * and/or read throttling exceptions with your own rate-limiting logic:
 *
 * <pre>{@code
 * // custom policy
 * public class MyCustomPolicy implements RateLimitPolicy {
 *
 *   public void onSuccess(List<KinesisRecord> records) throws InterruptedException {
 *     // handle successful getRecords() call
 *   }
 *
 *   public void onThrottle(KinesisClientThrottledException e) throws InterruptedException {
 *     // handle Kinesis read throttling exception
 *   }
 * }
 *
 * // custom factory
 * class MyCustomPolicyFactory implements RateLimitPolicyFactory {
 *
 *   public RateLimitPolicy getRateLimitPolicy() {
 *     return new MyCustomPolicy();
 *   }
 * }
 *
 * p.apply(KinesisIO.read()
 *    .withStreamName("streamName")
 *    .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *    .withCustomRateLimitPolicy(new MyCustomPolicyFactory())
 * }</pre>
 */
@Experimental(Kind.SOURCE_SINK)
public final class KinesisIO {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisIO.class);

  private static final int DEFAULT_NUM_RETRIES = 6;

  /** Returns a new {@link Read} transform for reading from Kinesis. */
  public static Read read() {
    return new AutoValue_KinesisIO_Read.Builder()
        .setMaxNumRecords(Long.MAX_VALUE)
        .setUpToDateThreshold(Duration.ZERO)
        .setWatermarkPolicyFactory(WatermarkPolicyFactory.withArrivalTimePolicy())
        .setRateLimitPolicyFactory(RateLimitPolicyFactory.withoutLimiter())
        .setMaxCapacityPerShard(ShardReadersPool.DEFAULT_CAPACITY_PER_SHARD)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KinesisRecord>> {

    abstract @Nullable String getStreamName();

    abstract @Nullable StartingPoint getInitialPosition();

    abstract @Nullable AWSClientsProvider getAWSClientsProvider();

    abstract long getMaxNumRecords();

    abstract @Nullable Duration getMaxReadTime();

    abstract Duration getUpToDateThreshold();

    abstract @Nullable Integer getRequestRecordsLimit();

    abstract WatermarkPolicyFactory getWatermarkPolicyFactory();

    abstract RateLimitPolicyFactory getRateLimitPolicyFactory();

    abstract Integer getMaxCapacityPerShard();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setStreamName(String streamName);

      abstract Builder setInitialPosition(StartingPoint startingPoint);

      abstract Builder setAWSClientsProvider(AWSClientsProvider clientProvider);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Builder setUpToDateThreshold(Duration upToDateThreshold);

      abstract Builder setRequestRecordsLimit(Integer limit);

      abstract Builder setWatermarkPolicyFactory(WatermarkPolicyFactory watermarkPolicyFactory);

      abstract Builder setRateLimitPolicyFactory(RateLimitPolicyFactory rateLimitPolicyFactory);

      abstract Builder setMaxCapacityPerShard(Integer maxCapacity);

      abstract Read build();
    }

    /** Specify reading from streamName. */
    public Read withStreamName(String streamName) {
      return toBuilder().setStreamName(streamName).build();
    }

    /** Specify reading from some initial position in stream. */
    public Read withInitialPositionInStream(InitialPositionInStream initialPosition) {
      return toBuilder().setInitialPosition(new StartingPoint(initialPosition)).build();
    }

    /**
     * Specify reading beginning at given {@link Instant}. This {@link Instant} must be in the past,
     * i.e. before {@link Instant#now()}.
     */
    public Read withInitialTimestampInStream(Instant initialTimestamp) {
      return toBuilder().setInitialPosition(new StartingPoint(initialTimestamp)).build();
    }

    /**
     * Allows to specify custom {@link AWSClientsProvider}. {@link AWSClientsProvider} provides
     * {@link KinesisClient} and {@link CloudWatchClient} instances which are later used for
     * communication with Kinesis. You should use this method if {@link
     * Read#withAWSClientsProvider(String, String, Region)} does not suit your needs.
     */
    public Read withAWSClientsProvider(AWSClientsProvider awsClientsProvider) {
      return toBuilder().setAWSClientsProvider(awsClientsProvider).build();
    }

    /**
     * Specify credential details and region to be used to read from Kinesis. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * Read#withAWSClientsProvider(AWSClientsProvider)}.
     */
    public Read withAWSClientsProvider(String awsAccessKey, String awsSecretKey, Region region) {
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
        String awsAccessKey, String awsSecretKey, Region region, String serviceEndpoint) {
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
     * decide to scale the amount of resources allocated to the pipeline in order to speed up
     * ingestion.
     */
    public Read withUpToDateThreshold(Duration upToDateThreshold) {
      checkArgument(upToDateThreshold != null, "upToDateThreshold can not be null");
      return toBuilder().setUpToDateThreshold(upToDateThreshold).build();
    }

    /**
     * Specifies the maximum number of records in GetRecordsResult returned by GetRecords call which
     * is limited by 10K records. If should be adjusted according to average size of data record to
     * prevent shard overloading. More details can be found here: <a
     * href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html">API_GetRecords</a>
     */
    public Read withRequestRecordsLimit(int limit) {
      checkArgument(limit > 0, "limit must be positive, but was: %s", limit);
      checkArgument(limit <= 10_000, "limit must be up to 10,000, but was: %s", limit);
      return toBuilder().setRequestRecordsLimit(limit).build();
    }

    /** Specifies the {@code WatermarkPolicyFactory} as ArrivalTimeWatermarkPolicyFactory. */
    public Read withArrivalTimeWatermarkPolicy() {
      return toBuilder()
          .setWatermarkPolicyFactory(WatermarkPolicyFactory.withArrivalTimePolicy())
          .build();
    }

    /**
     * Specifies the {@code WatermarkPolicyFactory} as ArrivalTimeWatermarkPolicyFactory.
     *
     * <p>{@param watermarkIdleDurationThreshold} Denotes the duration for which the watermark can
     * be idle.
     */
    public Read withArrivalTimeWatermarkPolicy(Duration watermarkIdleDurationThreshold) {
      return toBuilder()
          .setWatermarkPolicyFactory(
              WatermarkPolicyFactory.withArrivalTimePolicy(watermarkIdleDurationThreshold))
          .build();
    }

    /** Specifies the {@code WatermarkPolicyFactory} as ProcessingTimeWatermarkPolicyFactory. */
    public Read withProcessingTimeWatermarkPolicy() {
      return toBuilder()
          .setWatermarkPolicyFactory(WatermarkPolicyFactory.withProcessingTimePolicy())
          .build();
    }

    /**
     * Specifies the {@code WatermarkPolicyFactory} as a custom watermarkPolicyFactory.
     *
     * @param watermarkPolicyFactory Custom Watermark policy factory.
     */
    public Read withCustomWatermarkPolicy(WatermarkPolicyFactory watermarkPolicyFactory) {
      checkArgument(watermarkPolicyFactory != null, "watermarkPolicyFactory cannot be null");
      return toBuilder().setWatermarkPolicyFactory(watermarkPolicyFactory).build();
    }

    /** Specifies a fixed delay rate limit policy with the default delay of 1 second. */
    public Read withFixedDelayRateLimitPolicy() {
      return toBuilder().setRateLimitPolicyFactory(RateLimitPolicyFactory.withFixedDelay()).build();
    }

    /**
     * Specifies a fixed delay rate limit policy with the given delay.
     *
     * @param delay Denotes the fixed delay duration.
     */
    public Read withFixedDelayRateLimitPolicy(Duration delay) {
      checkArgument(delay != null, "delay cannot be null");
      return toBuilder()
          .setRateLimitPolicyFactory(RateLimitPolicyFactory.withFixedDelay(delay))
          .build();
    }

    /**
     * Specifies a dynamic delay rate limit policy with the given function being called at each
     * polling interval to get the next delay value. This can be used to change the polling interval
     * of a running pipeline based on some external configuration source, for example.
     *
     * @param delay The function to invoke to get the next delay duration.
     */
    public Read withDynamicDelayRateLimitPolicy(Supplier<Duration> delay) {
      checkArgument(delay != null, "delay cannot be null");
      return toBuilder().setRateLimitPolicyFactory(RateLimitPolicyFactory.withDelay(delay)).build();
    }

    /**
     * Specifies the {@code RateLimitPolicyFactory} for a custom rate limiter.
     *
     * @param rateLimitPolicyFactory Custom rate limit policy factory.
     */
    public Read withCustomRateLimitPolicy(RateLimitPolicyFactory rateLimitPolicyFactory) {
      checkArgument(rateLimitPolicyFactory != null, "rateLimitPolicyFactory cannot be null");
      return toBuilder().setRateLimitPolicyFactory(rateLimitPolicyFactory).build();
    }

    /** Specifies the maximum number of messages per one shard. */
    public Read withMaxCapacityPerShard(Integer maxCapacity) {
      checkArgument(maxCapacity > 0, "maxCapacity must be positive, but was: %s", maxCapacity);
      return toBuilder().setMaxCapacityPerShard(maxCapacity).build();
    }

    @Override
    public PCollection<KinesisRecord> expand(PBegin input) {
      Unbounded<KinesisRecord> unbounded =
          org.apache.beam.sdk.io.Read.from(
              new KinesisSource(
                  getAWSClientsProvider(),
                  getStreamName(),
                  getInitialPosition(),
                  getUpToDateThreshold(),
                  getWatermarkPolicyFactory(),
                  getRateLimitPolicyFactory(),
                  getRequestRecordsLimit(),
                  getMaxCapacityPerShard()));

      PTransform<PBegin, PCollection<KinesisRecord>> transform = unbounded;

      if (getMaxNumRecords() < Long.MAX_VALUE || getMaxReadTime() != null) {
        transform =
            unbounded.withMaxReadTime(getMaxReadTime()).withMaxNumRecords(getMaxNumRecords());
      }

      return input.apply(transform);
    }
  }
}
