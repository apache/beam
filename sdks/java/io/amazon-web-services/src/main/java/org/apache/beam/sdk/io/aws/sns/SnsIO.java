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
package org.apache.beam.sdk.io.aws.sns;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.GetTopicAttributesResult;
import com.amazonaws.services.sns.model.InternalErrorException;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.function.Predicate;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.http.HttpStatus;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for writing to <a href="https://aws.amazon.com/sns/">SNS</a>.
 *
 * <h3>Writing to SNS</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<PublishRequest> data = ...;
 *
 * data.apply(SnsIO.write()
 *     .withTopicName("topicName")
 *     .withRetryConfiguration(
 *        SnsIO.RetryConfiguration.create(
 *          4, org.joda.time.Duration.standardSeconds(10)))
 *     .withAWSClientsProvider(new BasicSnsProvider(accessKey, secretKey, region))
 *     .withResultOutputTag(results));
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>name of the SNS topic you're going to write to
 *   <li>retry configuration
 *   <li>need to specify AwsClientsProvider. You can pass on the default one BasicSnsProvider
 *   <li>an output tag where you can get results. Example in SnsIOTest
 * </ul>
 *
 * <p>By default, the output PublishResult contains only the messageId, all other fields are null.
 * If you need the full ResponseMetadata and SdkHttpMetadata you can call {@link
 * Write#withFullPublishResult}. If you need the HTTP status code but not the response headers you
 * can call {@link Write#withFullPublishResultWithoutHeaders}.
 */
@Experimental(Kind.SOURCE_SINK)
public final class SnsIO {

  // Write data tp SNS
  public static Write write() {
    return new AutoValue_SnsIO_Write.Builder().build();
  }

  /**
   * A POJO encapsulating a configuration for retry behavior when issuing requests to SNS. A retry
   * will be attempted until the maxAttempts or maxDuration is exceeded, whichever comes first, for
   * any of the following exceptions:
   *
   * <ul>
   *   <li>{@link IOException}
   * </ul>
   */
  @AutoValue
  public abstract static class RetryConfiguration implements Serializable {
    @VisibleForTesting
    static final RetryPredicate DEFAULT_RETRY_PREDICATE = new DefaultRetryPredicate();

    abstract int getMaxAttempts();

    abstract Duration getMaxDuration();

    abstract RetryPredicate getRetryPredicate();

    abstract Builder builder();

    public static RetryConfiguration create(int maxAttempts, Duration maxDuration) {
      checkArgument(maxAttempts > 0, "maxAttempts should be greater than 0");
      checkArgument(
          maxDuration != null && maxDuration.isLongerThan(Duration.ZERO),
          "maxDuration should be greater than 0");
      return new AutoValue_SnsIO_RetryConfiguration.Builder()
          .setMaxAttempts(maxAttempts)
          .setMaxDuration(maxDuration)
          .setRetryPredicate(DEFAULT_RETRY_PREDICATE)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract SnsIO.RetryConfiguration.Builder setMaxAttempts(int maxAttempts);

      abstract SnsIO.RetryConfiguration.Builder setMaxDuration(Duration maxDuration);

      abstract SnsIO.RetryConfiguration.Builder setRetryPredicate(RetryPredicate retryPredicate);

      abstract SnsIO.RetryConfiguration build();
    }

    /**
     * An interface used to control if we retry the SNS Publish call when a {@link Throwable}
     * occurs. If {@link RetryPredicate#test(Object)} returns true, {@link Write} tries to resend
     * the requests to SNS if the {@link RetryConfiguration} permits it.
     */
    @FunctionalInterface
    interface RetryPredicate extends Predicate<Throwable>, Serializable {}

    private static class DefaultRetryPredicate implements RetryPredicate {
      private static final ImmutableSet<Integer> ELIGIBLE_CODES =
          ImmutableSet.of(HttpStatus.SC_SERVICE_UNAVAILABLE);

      @Override
      public boolean test(Throwable throwable) {
        return (throwable instanceof IOException
            || (throwable instanceof InternalErrorException)
            || (throwable instanceof InternalErrorException
                && ELIGIBLE_CODES.contains(((InternalErrorException) throwable).getStatusCode())));
      }
    }
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<PublishRequest>, PCollectionTuple> {

    abstract @Nullable String getTopicName();

    abstract @Nullable AwsClientsProvider getAWSClientsProvider();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract @Nullable TupleTag<PublishResult> getResultOutputTag();

    abstract @Nullable Coder getCoder();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setTopicName(String topicName);

      abstract Builder setAWSClientsProvider(AwsClientsProvider clientProvider);

      abstract Builder setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Builder setResultOutputTag(TupleTag<PublishResult> results);

      abstract Builder setCoder(Coder coder);

      abstract Write build();
    }

    /**
     * Specify the SNS topic which will be used for writing, this name is mandatory.
     *
     * @param topicName topicName
     */
    public Write withTopicName(String topicName) {
      return builder().setTopicName(topicName).build();
    }

    /**
     * Allows to specify custom {@link AwsClientsProvider}. {@link AwsClientsProvider} creates new
     * {@link AmazonSNS} which is later used for writing to a SNS topic.
     */
    public Write withAWSClientsProvider(AwsClientsProvider awsClientsProvider) {
      return builder().setAWSClientsProvider(awsClientsProvider).build();
    }

    /**
     * Specify credential details and region to be used to write to SNS. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * Write#withAWSClientsProvider(AwsClientsProvider)}.
     */
    public Write withAWSClientsProvider(String awsAccessKey, String awsSecretKey, Regions region) {
      return withAWSClientsProvider(awsAccessKey, awsSecretKey, region, null);
    }

    /**
     * Specify credential details and region to be used to write to SNS. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * Write#withAWSClientsProvider(AwsClientsProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host. This is useful to execute
     * the tests with Kinesis service emulator.
     */
    public Write withAWSClientsProvider(
        String awsAccessKey, String awsSecretKey, Regions region, String serviceEndpoint) {
      return withAWSClientsProvider(
          new BasicSnsProvider(awsAccessKey, awsSecretKey, region, serviceEndpoint));
    }

    /**
     * Provides configuration to retry a failed request to publish a message to SNS. Users should
     * consider that retrying might compound the underlying problem which caused the initial
     * failure. Users should also be aware that once retrying is exhausted the error is surfaced to
     * the runner which <em>may</em> then opt to retry the current partition in entirety or abort if
     * the max number of retries of the runner is completed. Retrying uses an exponential backoff
     * algorithm, with minimum backoff of 5 seconds and then surfacing the error once the maximum
     * number of retries or maximum configuration duration is exceeded.
     *
     * <p>Example use:
     *
     * <pre>{@code
     * SnsIO.write()
     *   .withRetryConfiguration(SnsIO.RetryConfiguration.create(5, Duration.standardMinutes(1))
     *   ...
     * }</pre>
     *
     * @param retryConfiguration the rules which govern the retry behavior
     * @return the {@link Write} with retrying configured
     */
    public Write withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration is required");
      return builder().setRetryConfiguration(retryConfiguration).build();
    }

    /** Tuple tag to store results. Mandatory field. */
    public Write withResultOutputTag(TupleTag<PublishResult> results) {
      return builder().setResultOutputTag(results).build();
    }

    /**
     * Encode the full {@code PublishResult} object, including sdkResponseMetadata and
     * sdkHttpMetadata with the HTTP response headers.
     */
    public Write withFullPublishResult() {
      return withCoder(PublishResultCoders.fullPublishResult());
    }

    /**
     * Encode the full {@code PublishResult} object, including sdkResponseMetadata and
     * sdkHttpMetadata but excluding the HTTP response headers.
     */
    public Write withFullPublishResultWithoutHeaders() {
      return withCoder(PublishResultCoders.fullPublishResultWithoutHeaders());
    }

    /** Encode the {@code PublishResult} with the given coder. */
    public Write withCoder(Coder<PublishResult> coder) {
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollectionTuple expand(PCollection<PublishRequest> input) {
      checkArgument(getTopicName() != null, "withTopicName() is required");
      PCollectionTuple result =
          input.apply(
              ParDo.of(new SnsWriterFn(this))
                  .withOutputTags(getResultOutputTag(), TupleTagList.empty()));
      if (getCoder() != null) {
        result.get(getResultOutputTag()).setCoder(getCoder());
      }
      return result;
    }

    static class SnsWriterFn extends DoFn<PublishRequest, PublishResult> {
      @VisibleForTesting
      static final String RETRY_ATTEMPT_LOG = "Error writing to SNS. Retry attempt[%d]";

      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);
      private transient FluentBackoff retryBackoff; // defaults to no retries
      private static final Logger LOG = LoggerFactory.getLogger(SnsWriterFn.class);
      private static final Counter SNS_WRITE_FAILURES =
          Metrics.counter(SnsWriterFn.class, "SNS_Write_Failures");

      private final SnsIO.Write spec;
      private transient AmazonSNS producer;

      SnsWriterFn(SnsIO.Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        // Initialize SnsPublisher
        producer = spec.getAWSClientsProvider().createSnsPublisher();
        checkArgument(
            topicExists(producer, spec.getTopicName()),
            "Topic %s does not exist",
            spec.getTopicName());

        retryBackoff =
            FluentBackoff.DEFAULT
                .withMaxRetries(0) // default to no retrying
                .withInitialBackoff(RETRY_INITIAL_BACKOFF);
        if (spec.getRetryConfiguration() != null) {
          retryBackoff =
              retryBackoff
                  .withMaxRetries(spec.getRetryConfiguration().getMaxAttempts() - 1)
                  .withMaxCumulativeBackoff(spec.getRetryConfiguration().getMaxDuration());
        }
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        PublishRequest request = context.element();
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = retryBackoff.backoff();
        int attempt = 0;
        while (true) {
          attempt++;
          try {
            PublishResult pr = producer.publish(request);
            context.output(pr);
            break;
          } catch (Exception ex) {
            // Fail right away if there is no retry configuration
            if (spec.getRetryConfiguration() == null
                || !spec.getRetryConfiguration().getRetryPredicate().test(ex)) {
              SNS_WRITE_FAILURES.inc();
              LOG.info("Unable to publish message {}.", request.getMessage(), ex);
              throw new IOException("Error writing to SNS (no attempt made to retry)", ex);
            }

            if (!BackOffUtils.next(sleeper, backoff)) {
              throw new IOException(
                  String.format(
                      "Error writing to SNS after %d attempt(s). No more attempts allowed",
                      attempt),
                  ex);
            } else {
              // Note: this used in test cases to verify behavior
              LOG.warn(String.format(RETRY_ATTEMPT_LOG, attempt), ex);
            }
          }
        }
      }

      @Teardown
      public void tearDown() {
        if (producer != null) {
          producer.shutdown();
          producer = null;
        }
      }

      @SuppressWarnings({"checkstyle:illegalCatch"})
      private static boolean topicExists(AmazonSNS client, String topicName) {
        try {
          GetTopicAttributesResult topicAttributesResult = client.getTopicAttributes(topicName);
          return topicAttributesResult != null
              && topicAttributesResult.getSdkHttpMetadata().getHttpStatusCode() == 200;
        } catch (Exception e) {
          LOG.warn("Error checking whether topic {} exists.", topicName, e);
          throw e;
        }
      }
    }
  }
}
