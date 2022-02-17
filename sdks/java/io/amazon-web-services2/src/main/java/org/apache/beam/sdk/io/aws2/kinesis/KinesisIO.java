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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.net.URI;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.Read.Unbounded;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.RateLimitPolicyFactory.DefaultRateLimiter;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.kinesis.common.InitialPositionInStream;

/**
 * IO to read from <a href="https://aws.amazon.com/kinesis/">Kinesis</a> streams.
 *
 * <p>Note that KinesisIO.Write is based on the Kinesis Producer Library which does not yet have an
 * update to be compatible with AWS SDK for Java version 2 so for now the version in {@code
 * org.apache.beam.sdk.io.kinesis} should be used for writing to Kinesis.
 *
 * <h3>Reading from Kinesis</h3>
 *
 * <p>Example usages:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *     .withStreamName("streamName")
 *     .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *  .apply( ... ) // other transformations
 * }</pre>
 *
 * <p>At a minimum you have to provide:
 *
 * <ul>
 *   <li>the name of the stream to read
 *   <li>the position in the stream where to start reading, e.g. {@link
 *       InitialPositionInStream#LATEST}, {@link InitialPositionInStream#TRIM_HORIZON}, or
 *       alternatively, using an arbitrary point in time with {@link
 *       Read#withInitialTimestampInStream(Instant)}.
 * </ul>
 *
 * <h4>Watermarks</h4>
 *
 * <p>Kinesis IO uses arrival time for watermarks by default. To use processing time instead, use
 * {@link Read#withProcessingTimeWatermarkPolicy()}:
 *
 * <pre>{@code
 * p.apply(KinesisIO.read()
 *    .withStreamName("streamName")
 *    .withInitialPositionInStream(InitialPositionInStream.LATEST)
 *    .withProcessingTimeWatermarkPolicy())
 * }</pre>
 *
 * <p>It is also possible to specify a custom watermark policy to control watermark computation
 * using {@link Read#withCustomWatermarkPolicy(WatermarkPolicyFactory)}. This requires implementing
 * {@link WatermarkPolicy} with a corresponding {@link WatermarkPolicyFactory}.
 *
 * <h4>Throttling</h4>
 *
 * <p>By default Kinesis IO will poll the Kinesis {@code getRecords()} API as fast as possible as
 * long as records are returned. The {@link DefaultRateLimiter} will start throttling once {@code
 * getRecords()} returns an empty response or if API calls get throttled by AWS.
 *
 * <p>A {@link RateLimitPolicy} is always applied to each shard individually.
 *
 * <p>You may provide a custom rate limit policy using {@link
 * Read#withCustomRateLimitPolicy(RateLimitPolicyFactory)}. This requires implementing {@link
 * RateLimitPolicy} with a corresponding {@link RateLimitPolicyFactory}.
 *
 * <h3>Configuration of AWS clients</h3>
 *
 * <p>AWS clients for all AWS IOs can be configured using {@link AwsOptions}, e.g. {@code
 * --awsRegion=us-west-1}. {@link AwsOptions} contain reasonable defaults based on default providers
 * for {@link Region} and {@link AwsCredentialsProvider}.
 *
 * <p>If you require more advanced configuration, you may change the {@link ClientBuilderFactory}
 * using {@link AwsOptions#setClientBuilderFactory(Class)}.
 *
 * <p>Configuration for a specific IO can be overwritten using {@code withClientConfiguration()},
 * which also allows to configure the retry behavior for the respective IO.
 *
 * <h4>Retries</h4>
 *
 * <p>Retries for failed requests can be configured using {@link
 * ClientConfiguration.Builder#retry(Consumer)} and are handled by the AWS SDK unless there's a
 * partial success (batch requests). The SDK uses a backoff strategy with equal jitter for computing
 * the delay before the next retry.
 *
 * <p><b>Note:</b> Once retries are exhausted the error is surfaced to the runner which <em>may</em>
 * then opt to retry the current partition in entirety or abort if the max number of retries of the
 * runner is reached.
 */
@Experimental(Kind.SOURCE_SINK)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public final class KinesisIO {

  /** Returns a new {@link Read} transform for reading from Kinesis. */
  public static Read read() {
    return new AutoValue_KinesisIO_Read.Builder()
        .setClientConfiguration(ClientConfiguration.builder().build())
        .setMaxNumRecords(Long.MAX_VALUE)
        .setUpToDateThreshold(Duration.ZERO)
        .setWatermarkPolicyFactory(WatermarkPolicyFactory.withArrivalTimePolicy())
        .setRateLimitPolicyFactory(RateLimitPolicyFactory.withDefaultRateLimiter())
        .setMaxCapacityPerShard(ShardReadersPool.DEFAULT_CAPACITY_PER_SHARD)
        .build();
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KinesisRecord>> {

    abstract @Nullable String getStreamName();

    abstract @Nullable StartingPoint getInitialPosition();

    abstract @Nullable ClientConfiguration getClientConfiguration();

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

      abstract Builder setClientConfiguration(ClientConfiguration config);

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
     * @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. Alternatively
     *     you can configure a custom {@link ClientBuilderFactory} in {@link AwsOptions}.
     */
    @Deprecated
    public Read withAWSClientsProvider(AWSClientsProvider clientProvider) {
      checkArgument(clientProvider != null, "AWSClientsProvider cannot be null");
      return toBuilder().setClientConfiguration(null).setAWSClientsProvider(clientProvider).build();
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Read withAWSClientsProvider(String awsAccessKey, String awsSecretKey, Region region) {
      return withAWSClientsProvider(awsAccessKey, awsSecretKey, region, null);
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Read withAWSClientsProvider(
        String awsAccessKey, String awsSecretKey, Region region, String serviceEndpoint) {
      AwsCredentialsProvider awsCredentialsProvider =
          StaticCredentialsProvider.create(AwsBasicCredentials.create(awsAccessKey, awsSecretKey));
      return withAWSClientsProvider(awsCredentialsProvider, region, serviceEndpoint);
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Read withAWSClientsProvider(
        AwsCredentialsProvider awsCredentialsProvider, Region region) {
      return withAWSClientsProvider(awsCredentialsProvider, region, null);
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Read withAWSClientsProvider(
        AwsCredentialsProvider awsCredentialsProvider, Region region, String serviceEndpoint) {
      URI endpoint = serviceEndpoint != null ? URI.create(serviceEndpoint) : null;
      return updateClientConfig(
          b ->
              b.credentialsProvider(awsCredentialsProvider)
                  .region(region)
                  .endpoint(endpoint)
                  .build());
    }

    /** Configuration of Kinesis & Cloudwatch clients. */
    public Read withClientConfiguration(ClientConfiguration config) {
      return updateClientConfig(ignore -> config);
    }

    private Read updateClientConfig(Function<ClientConfiguration.Builder, ClientConfiguration> fn) {
      checkState(
          getAWSClientsProvider() == null,
          "Legacy AWSClientsProvider is set, but incompatible with ClientConfiguration.");
      ClientConfiguration config = fn.apply(getClientConfiguration().toBuilder());
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return toBuilder().setClientConfiguration(config).build();
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
      checkArgument(getWatermarkPolicyFactory() != null, "WatermarkPolicyFactory is required");
      checkArgument(getRateLimitPolicyFactory() != null, "RateLimitPolicyFactory is required");
      if (getAWSClientsProvider() == null) {
        checkArgument(getClientConfiguration() != null, "ClientConfiguration is required");
        AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
        ClientBuilderFactory.validate(awsOptions, getClientConfiguration());
      }

      Unbounded<KinesisRecord> unbounded =
          org.apache.beam.sdk.io.Read.from(new KinesisSource(this));

      PTransform<PBegin, PCollection<KinesisRecord>> transform = unbounded;

      if (getMaxNumRecords() < Long.MAX_VALUE || getMaxReadTime() != null) {
        transform =
            unbounded.withMaxReadTime(getMaxReadTime()).withMaxNumRecords(getMaxNumRecords());
      }

      return input.apply(transform);
    }
  }
}
