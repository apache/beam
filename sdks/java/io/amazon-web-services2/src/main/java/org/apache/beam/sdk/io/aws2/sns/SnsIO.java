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
package org.apache.beam.sdk.io.aws2.sns;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.http.HttpStatus;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesRequest;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesResponse;
import software.amazon.awssdk.services.sns.model.InternalErrorException;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * {@link PTransform}s for writing to <a href="https://aws.amazon.com/sns/">SNS</a>.
 *
 * <h3>Writing to SNS Synchronously</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<String> data = ...;
 *
 * data.apply(SnsIO.<String>write()
 *     .withPublishRequestFn(m -> PublishRequest.builder().topicArn("topicArn").message(m).build())
 *     .withTopicArn("topicArn")
 *     .withRetryConfiguration(
 *        SnsIO.RetryConfiguration.create(
 *          4, org.joda.time.Duration.standardSeconds(10)))
 *     .withSnsClientProvider(new BasicSnsClientProvider(awsCredentialsProvider, region));
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>SNS topic arn you're going to publish to
 *   <li>Retry Configuration
 *   <li>AwsCredentialsProvider, which you can pass on to BasicSnsClientProvider
 *   <li>publishRequestFn, a function to convert your message into PublishRequest
 * </ul>
 *
 * <h3>Writing to SNS Asynchronously</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<String> data = ...;
 *
 * data.apply(SnsIO.<String>writeAsync()
 * 		.withElementCoder(StringUtf8Coder.of())
 * 		.withPublishRequestFn(createPublishRequestFn())
 * 		.withSnsClientProvider(new BasicSnsClientProvider(awsCredentialsProvider, region));
 *
 * }</pre>
 *
 * <pre>{@code
 * PCollection<String> data = ...;
 *
 * PCollection<SnsResponse<String>> responses = data.apply(SnsIO.<String>writeAsync()
 *      .withElementCoder(StringUtf8Coder.of())
 *      .withPublishRequestFn(createPublishRequestFn())
 *  *   .withSnsClientProvider(new BasicSnsClientProvider(awsCredentialsProvider, region));
 *
 * }</pre>
 *
 * <p>As a client, you need to provide at least the following things:
 *
 * <ul>
 *   <li>Coder for element T.
 *   <li>publishRequestFn, a function to convert your message into PublishRequest
 *   <li>SnsClientProvider, a provider to create an async client.
 * </ul>
 */
@Experimental(Kind.SOURCE_SINK)
public final class SnsIO {

  // Write data to SNS (synchronous)
  public static <T> Write<T> write() {
    return new AutoValue_SnsIO_Write.Builder().build();
  }

  public static <T> WriteAsync<T> writeAsync() {
    return new AutoValue_SnsIO_WriteAsync.Builder().build();
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
      abstract Builder setMaxAttempts(int maxAttempts);

      abstract Builder setMaxDuration(Duration maxDuration);

      abstract Builder setRetryPredicate(RetryPredicate retryPredicate);

      abstract RetryConfiguration build();
    }

    /**
     * An interface used to control if we retry the SNS Publish call when a {@link Throwable}
     * occurs. If {@link RetryPredicate#test(Object)} returns true, {@link Write} tries to resend
     * the requests to the Solr server if the {@link RetryConfiguration} permits it.
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
                && ELIGIBLE_CODES.contains(((InternalErrorException) throwable).statusCode())));
      }
    }
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T>
      extends PTransform<PCollection<T>, PCollection<PublishResponse>> {

    abstract @Nullable String getTopicArn();

    abstract @Nullable SerializableFunction<T, PublishRequest> getPublishRequestFn();

    abstract @Nullable SnsClientProvider getSnsClientProvider();

    abstract @Nullable RetryConfiguration getRetryConfiguration();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setTopicArn(String topicArn);

      abstract Builder<T> setPublishRequestFn(
          SerializableFunction<T, PublishRequest> publishRequestFn);

      abstract Builder<T> setSnsClientProvider(SnsClientProvider snsClientProvider);

      abstract Builder<T> setRetryConfiguration(RetryConfiguration retryConfiguration);

      abstract Write<T> build();
    }

    /**
     * Specify the SNS topic which will be used for writing, this name is mandatory.
     *
     * @param topicArn topicArn
     */
    public Write<T> withTopicArn(String topicArn) {
      return builder().setTopicArn(topicArn).build();
    }

    /**
     * Specify a function for converting a message into PublishRequest object, this function is
     * mandatory.
     *
     * @param publishRequestFn publishRequestFn
     */
    public Write<T> withPublishRequestFn(SerializableFunction<T, PublishRequest> publishRequestFn) {
      return builder().setPublishRequestFn(publishRequestFn).build();
    }

    /**
     * Allows to specify custom {@link SnsClientProvider}. {@link SnsClientProvider} creates new
     * {@link SnsClient} which is later used for writing to a SNS topic.
     */
    public Write<T> withSnsClientProvider(SnsClientProvider awsClientsProvider) {
      return builder().setSnsClientProvider(awsClientsProvider).build();
    }

    /**
     * Specify {@link AwsCredentialsProvider} and region to be used to write to SNS. If you need
     * more sophisticated credential protocol, then you should look at {@link
     * Write#withSnsClientProvider(SnsClientProvider)}.
     */
    public Write<T> withSnsClientProvider(
        AwsCredentialsProvider credentialsProvider, String region) {
      return withSnsClientProvider(credentialsProvider, region, null);
    }

    /**
     * Specify {@link AwsCredentialsProvider} and region to be used to write to SNS. If you need
     * more sophisticated credential protocol, then you should look at {@link
     * Write#withSnsClientProvider(SnsClientProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host. This is useful to execute
     * the tests with Kinesis service emulator.
     */
    public Write<T> withSnsClientProvider(
        AwsCredentialsProvider credentialsProvider, String region, URI serviceEndpoint) {
      return withSnsClientProvider(
          new BasicSnsClientProvider(credentialsProvider, region, serviceEndpoint));
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
    public Write<T> withRetryConfiguration(RetryConfiguration retryConfiguration) {
      checkArgument(retryConfiguration != null, "retryConfiguration is required");
      return builder().setRetryConfiguration(retryConfiguration).build();
    }

    private static boolean isTopicExists(SnsClient client, String topicArn) {
      try {
        GetTopicAttributesRequest getTopicAttributesRequest =
            GetTopicAttributesRequest.builder().topicArn(topicArn).build();
        GetTopicAttributesResponse topicAttributesResponse =
            client.getTopicAttributes(getTopicAttributesRequest);
        return topicAttributesResponse != null
            && topicAttributesResponse.sdkHttpResponse().statusCode() == 200;
      } catch (Exception e) {
        throw e;
      }
    }

    @Override
    public PCollection<PublishResponse> expand(PCollection<T> input) {
      checkArgument(getTopicArn() != null, "withTopicArn() is required");
      checkArgument(getPublishRequestFn() != null, "withPublishRequestFn() is required");
      checkArgument(getSnsClientProvider() != null, "withSnsClientProvider() is required");
      checkArgument(
          isTopicExists(getSnsClientProvider().getSnsClient(), getTopicArn()),
          "Topic arn %s does not exist",
          getTopicArn());

      return input.apply(ParDo.of(new SnsWriterFn<>(this)));
    }

    static class SnsWriterFn<T> extends DoFn<T, PublishResponse> {
      @VisibleForTesting
      static final String RETRY_ATTEMPT_LOG = "Error writing to SNS. Retry attempt[%d]";

      private static final Duration RETRY_INITIAL_BACKOFF = Duration.standardSeconds(5);
      private transient FluentBackoff retryBackoff; // defaults to no retries
      private static final Logger LOG = LoggerFactory.getLogger(SnsWriterFn.class);
      private static final Counter SNS_WRITE_FAILURES =
          Metrics.counter(SnsWriterFn.class, "SNS_Write_Failures");

      private final Write spec;
      private transient SnsClient producer;

      SnsWriterFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        // Initialize SnsPublisher
        producer = spec.getSnsClientProvider().getSnsClient();

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
        PublishRequest request =
            (PublishRequest) spec.getPublishRequestFn().apply(context.element());
        Sleeper sleeper = Sleeper.DEFAULT;
        BackOff backoff = retryBackoff.backoff();
        int attempt = 0;
        while (true) {
          attempt++;
          try {
            PublishResponse pr = producer.publish(request);
            context.output(pr);
            break;
          } catch (Exception ex) {
            // Fail right away if there is no retry configuration
            if (spec.getRetryConfiguration() == null
                || !spec.getRetryConfiguration().getRetryPredicate().test(ex)) {
              SNS_WRITE_FAILURES.inc();
              LOG.info("Unable to publish message {}.", request.message(), ex);
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
          producer.close();
          producer = null;
        }
      }
    }
  }

  /** Implementation of {@link #writeAsync}. */
  @AutoValue
  public abstract static class WriteAsync<T>
      extends PTransform<PCollection<T>, PCollection<SnsResponse<T>>> {

    abstract @Nullable SnsAsyncClientProvider getSnsClientProvider();

    /** SerializableFunction to create PublishRequest. */
    abstract @Nullable SerializableFunction<T, PublishRequest> getPublishRequestFn();

    /** Coder for element T. */
    abstract @Nullable Coder<T> getCoder();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setSnsClientProvider(SnsAsyncClientProvider asyncClientProvider);

      abstract Builder<T> setCoder(Coder<T> elementCoder);

      abstract Builder<T> setPublishRequestFn(
          SerializableFunction<T, PublishRequest> publishRequestFn);

      abstract WriteAsync<T> build();
    }

    /**
     * Specify a Coder for SNS PublishRequest object.
     *
     * @param elementCoder Coder
     */
    public WriteAsync<T> withCoder(Coder<T> elementCoder) {
      checkNotNull(elementCoder, "elementCoder cannot be null");
      return builder().setCoder(elementCoder).build();
    }

    /**
     * Specify a function for converting a message into PublishRequest object.
     *
     * @param publishRequestFn publishRequestFn
     */
    public WriteAsync<T> withPublishRequestFn(
        SerializableFunction<T, PublishRequest> publishRequestFn) {
      checkNotNull(publishRequestFn, "publishRequestFn cannot be null");
      return builder().setPublishRequestFn(publishRequestFn).build();
    }

    /**
     * Allows to specify custom {@link SnsAsyncClientProvider}. {@link SnsAsyncClientProvider}
     * creates new {@link SnsAsyncClientProvider} which is later used for writing to a SNS topic.
     */
    public WriteAsync<T> withSnsClientProvider(SnsAsyncClientProvider asyncClientProvider) {
      checkNotNull(asyncClientProvider, "asyncClientProvider cannot be null");
      return builder().setSnsClientProvider(asyncClientProvider).build();
    }

    /**
     * Specify credential details and region to be used to write to SNS. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * WriteAsync#withSnsClientProvider(SnsAsyncClientProvider)}.
     */
    public WriteAsync<T> withSnsClientProvider(
        AwsCredentialsProvider credentialsProvider, String region) {
      checkNotNull(credentialsProvider, "credentialsProvider cannot be null");
      checkNotNull(region, "region cannot be null");
      return withSnsClientProvider(credentialsProvider, region, null);
    }

    /**
     * Specify credential details and region to be used to write to SNS. If you need more
     * sophisticated credential protocol, then you should look at {@link
     * WriteAsync#withSnsClientProvider(SnsAsyncClientProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host.
     */
    public WriteAsync<T> withSnsClientProvider(
        AwsCredentialsProvider credentialsProvider, String region, URI serviceEndpoint) {
      checkNotNull(credentialsProvider, "credentialsProvider cannot be null");
      checkNotNull(region, "region cannot be null");
      return withSnsClientProvider(
          new BasicSnsAsyncClientProvider(credentialsProvider, region, serviceEndpoint));
    }

    @Override
    public PCollection<SnsResponse<T>> expand(PCollection<T> input) {
      checkArgument(getSnsClientProvider() != null, "withSnsClientProvider() needs to called");
      checkArgument(getPublishRequestFn() != null, "withPublishRequestFn() needs to called");
      checkArgument(getCoder() != null, "withElementCoder() needs to called");

      return input
          .apply(ParDo.of(new SnsWriteAsyncFn<>(this)))
          .setCoder(SnsResponseCoder.of(getCoder()));
    }

    private static class SnsWriteAsyncFn<T> extends DoFn<T, SnsResponse<T>> {

      private static final Logger LOG = LoggerFactory.getLogger(SnsWriteAsyncFn.class);

      private final WriteAsync<T> spec;
      private transient SnsAsyncClient client;

      SnsWriteAsyncFn(WriteAsync<T> spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() {
        this.client = spec.getSnsClientProvider().getSnsAsyncClient();
      }

      @SuppressWarnings("FutureReturnValueIgnored")
      @ProcessElement
      public void processElement(ProcessContext context) {
        PublishRequest publishRequest = spec.getPublishRequestFn().apply(context.element());
        client.publish(publishRequest).whenComplete(getPublishResponse(context));
      }

      private BiConsumer<? super PublishResponse, ? super Throwable> getPublishResponse(
          DoFn<T, SnsResponse<T>>.ProcessContext context) {
        return (response, ex) -> {
          if (ex == null) {
            SnsResponse<T> snsResponse = SnsResponse.of(context.element(), response);
            context.output(snsResponse);
          } else {
            LOG.error("Error while publishing request to SNS", ex);
            throw new SnsWriteException("Error while publishing request to SNS", ex);
          }
        };
      }
    }
  }

  /** Exception class for SNS write exceptions. */
  protected static class SnsWriteException extends RuntimeException {

    SnsWriteException(String message, Throwable error) {
      super(message, error);
    }
  }
}
