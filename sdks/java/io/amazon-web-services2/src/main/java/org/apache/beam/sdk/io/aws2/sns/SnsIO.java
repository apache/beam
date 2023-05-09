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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.net.URI;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.io.aws2.schemas.AwsSchemaProvider;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsResponseMetadata;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.InvalidParameterException;
import software.amazon.awssdk.services.sns.model.NotFoundException;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * IO to send notifications via <a href="https://aws.amazon.com/sns/">SNS</a>.
 *
 * <h3>Writing to SNS</h3>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<String> data = ...;
 * data.apply(SnsIO.<String>write()
 *     .withTopicArn("topicArn")
 *     .withPublishRequestBuilder(msg -> PublishRequest.builder().message(msg)));
 * }</pre>
 *
 * <p>At a minimum you have to provide:
 *
 * <ul>
 *   <li>SNS topic ARN you're going to publish to (optional, but required for most use cases)
 *   <li>Request builder function to create SNS publish requests from your input
 * </ul>
 *
 * <p>By default, the output {@link PublishResponse} contains only the SNS messageId, all other
 * fields are null. If you need to include the full {@link SdkHttpResponse} and {@link
 * AwsResponseMetadata}, you can call {@link Write#withFullPublishResponse()}. If you need the HTTP
 * status code only but no headers, you can use {@link
 * Write#withFullPublishResponseWithoutHeaders()}.
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
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class SnsIO {

  // Write data to SNS
  public static <T> Write<T> write() {
    return new AutoValue_SnsIO_Write.Builder<T>()
        .setClientConfiguration(ClientConfiguration.builder().build())
        .build();
  }

  /**
   * @deprecated Please use {@link SnsIO#write()} to avoid the risk of data loss.
   * @see <a href="https://github.com/apache/beam/issues/21366">Issue #21366</a>, <a
   *     href="https://issues.apache.org/jira/browse/BEAM-13203">BEAM-13203</a>
   */
  @Deprecated
  public static <T> WriteAsync<T> writeAsync() {
    return new AutoValue_SnsIO_WriteAsync.Builder<T>().build();
  }

  /**
   * Legacy retry configuration.
   *
   * <p><b>Warning</b>: Max accumulative retry latency is silently ignored as it is not supported by
   * the AWS SDK.
   *
   * @deprecated Use {@link org.apache.beam.sdk.io.aws2.common.RetryConfiguration} instead to
   *     delegate retries to the AWS SDK.
   */
  @AutoValue
  @Deprecated
  public abstract static class RetryConfiguration implements Serializable {
    abstract int getMaxAttempts();

    /** @deprecated Use {@link org.apache.beam.sdk.io.aws2.common.RetryConfiguration} instead. */
    @Deprecated
    public static RetryConfiguration create(int maxAttempts, Duration ignored) {
      checkArgument(maxAttempts > 0, "maxAttempts should be greater than 0");
      return new AutoValue_SnsIO_RetryConfiguration(maxAttempts);
    }

    org.apache.beam.sdk.io.aws2.common.RetryConfiguration convertLegacyConfig() {
      int totalAttempts = getMaxAttempts() * 3; // 3 SDK attempts per user attempt
      return org.apache.beam.sdk.io.aws2.common.RetryConfiguration.builder()
          .numRetries(totalAttempts - 1)
          .build();
    }
  }

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T>
      extends PTransform<PCollection<T>, PCollection<PublishResponse>> {

    abstract @Nullable ClientConfiguration getClientConfiguration();

    abstract @Nullable String getTopicArn();

    abstract @Nullable SerializableFunction<T, PublishRequest.Builder> getPublishRequestBuilder();

    abstract @Nullable SnsClientProvider getSnsClientProvider();

    abstract @Nullable Coder<PublishResponse> getCoder();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setClientConfiguration(ClientConfiguration config);

      abstract Builder<T> setTopicArn(String topicArn);

      abstract Builder<T> setPublishRequestBuilder(
          SerializableFunction<T, PublishRequest.Builder> requestBuilder);

      abstract Builder<T> setSnsClientProvider(SnsClientProvider snsClientProvider);

      abstract Builder<T> setCoder(Coder<PublishResponse> coder);

      abstract Write<T> build();
    }

    /**
     * SNS topic ARN used for publishing to SNS.
     *
     * <p>The topic ARN is optional. If set, its existence will be validated and the SNS publish
     * request will be configured accordingly.
     */
    public Write<T> withTopicArn(String topicArn) {
      return builder().setTopicArn(topicArn).build();
    }

    /**
     * Function to convert a message into a {@link PublishRequest.Builder} (mandatory).
     *
     * <p>If an SNS topic arn is set, it will be automatically set on the {@link
     * PublishRequest.Builder}.
     */
    public Write<T> withPublishRequestBuilder(
        SerializableFunction<T, PublishRequest.Builder> requestBuilder) {
      return builder().setPublishRequestBuilder(requestBuilder).build();
    }

    /**
     * Specify a function for converting a message into PublishRequest object.
     *
     * @deprecated Use {@link #withPublishRequestBuilder(SerializableFunction)} instead.
     */
    @Deprecated
    public Write<T> withPublishRequestFn(SerializableFunction<T, PublishRequest> publishRequestFn) {
      return builder().setPublishRequestBuilder(m -> publishRequestFn.apply(m).toBuilder()).build();
    }

    /**
     * @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. Alternatively
     *     you can configure a custom {@link ClientBuilderFactory} in {@link AwsOptions}.
     */
    @Deprecated
    public Write<T> withSnsClientProvider(SnsClientProvider clientProvider) {
      checkArgument(clientProvider != null, "SnsClientProvider cannot be null");
      return builder().setClientConfiguration(null).setSnsClientProvider(clientProvider).build();
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Write<T> withSnsClientProvider(AwsCredentialsProvider credentials, String region) {
      return updateClientConfig(
          b -> b.credentialsProvider(credentials).region(Region.of(region)).build());
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Write<T> withSnsClientProvider(
        AwsCredentialsProvider credentials, String region, URI endpoint) {
      return updateClientConfig(
          b ->
              b.credentialsProvider(credentials)
                  .region(Region.of(region))
                  .endpoint(endpoint)
                  .build());
    }

    /** Configuration of SNS client. */
    public Write<T> withClientConfiguration(ClientConfiguration config) {
      return updateClientConfig(ignore -> config);
    }

    private Write<T> updateClientConfig(
        Function<ClientConfiguration.Builder, ClientConfiguration> fn) {
      checkState(
          getSnsClientProvider() == null,
          "Legacy SnsClientProvider is set, but incompatible with ClientConfiguration.");
      ClientConfiguration config = fn.apply(getClientConfiguration().toBuilder());
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return builder().setClientConfiguration(config).build();
    }

    /**
     * Retry configuration of SNS client.
     *
     * @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} with {@link
     *     org.apache.beam.sdk.io.aws2.common.RetryConfiguration} instead to delegate retries to the
     *     AWS SDK.
     */
    @Deprecated
    public Write<T> withRetryConfiguration(RetryConfiguration retry) {
      return updateClientConfig(b -> b.retry(retry.convertLegacyConfig()).build());
    }

    /**
     * Encode the full {@link PublishResponse} object, including sdkResponseMetadata and
     * sdkHttpMetadata with the HTTP response headers.
     *
     * @deprecated Writes fail exceptionally in case of errors, there is no need to check headers.
     */
    @Deprecated
    public Write<T> withFullPublishResponse() {
      return withCoder(PublishResponseCoders.fullPublishResponse());
    }

    /**
     * Encode the full {@link PublishResponse} object, including sdkResponseMetadata and
     * sdkHttpMetadata but excluding the HTTP response headers.
     *
     * @deprecated Writes fail exceptionally in case of errors, there is no need to check headers.
     */
    @Deprecated
    public Write<T> withFullPublishResponseWithoutHeaders() {
      return withCoder(PublishResponseCoders.fullPublishResponseWithoutHeaders());
    }

    /**
     * Encode the {@link PublishResponse} with the given coder.
     *
     * @deprecated Explicit usage of coders is deprecated. Inferred schemas provided by {@link
     *     AwsSchemaProvider} will be used instead.
     */
    @Deprecated
    public Write<T> withCoder(Coder<PublishResponse> coder) {
      return builder().setCoder(coder).build();
    }

    @Override
    public PCollection<PublishResponse> expand(PCollection<T> input) {
      checkArgument(getPublishRequestBuilder() != null, "withPublishRequestBuilder() is required");

      AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
      if (getSnsClientProvider() == null) {
        checkArgument(getClientConfiguration() != null, "withClientConfiguration() is required");
        ClientBuilderFactory.validate(awsOptions, getClientConfiguration());
      }
      if (getTopicArn() != null) {
        checkArgument(checkTopicExists(awsOptions), "Topic arn %s does not exist", getTopicArn());
      }

      PCollection<PublishResponse> result = input.apply(ParDo.of(new SnsWriterFn<>(this)));
      if (getCoder() != null) {
        result.setCoder(getCoder());
      }
      return result;
    }

    private boolean checkTopicExists(AwsOptions options) {
      try (SnsClient client = buildClient(options)) {
        client.getTopicAttributes(b -> b.topicArn(getTopicArn()));
        return true;
      } catch (NotFoundException | InvalidParameterException e) {
        LoggerFactory.getLogger(Write.class)
            .warn("Configured topic ARN '" + getTopicArn() + "' does not exist.", e);
        return false;
      }
    }

    private SnsClient buildClient(AwsOptions options) {
      if (getSnsClientProvider() != null) {
        // build client using legacy SnsClientProvider
        return getSnsClientProvider().getSnsClient();
      } else {
        return ClientBuilderFactory.buildClient(
            options.as(AwsOptions.class), SnsClient.builder(), getClientConfiguration());
      }
    }

    static class SnsWriterFn<T> extends DoFn<T, PublishResponse> {
      private static final Logger LOG = LoggerFactory.getLogger(SnsWriterFn.class);
      private static final Counter SNS_WRITE_FAILURES =
          Metrics.counter(SnsWriterFn.class, "SNS_Write_Failures");

      private final Write<T> spec;
      private transient SnsClient producer;

      SnsWriterFn(Write<T> spec) {
        this.spec = spec;
      }

      @Setup
      public void setup(PipelineOptions options) throws Exception {
        producer = spec.buildClient(options.as(AwsOptions.class));
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        PublishRequest.Builder reqBuilder =
            spec.getPublishRequestBuilder().apply(context.element());

        if (spec.getTopicArn() != null) {
          reqBuilder.topicArn(spec.getTopicArn());
        }

        PublishRequest request = reqBuilder.build();

        try {
          PublishResponse pr = producer.publish(request);
          context.output(pr);
        } catch (SdkException e) {
          SNS_WRITE_FAILURES.inc();
          LOG.error("Unable to publish message {}.", request.message());
          throw e;
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

  /**
   * Implementation of {@link #writeAsync}.
   *
   * @deprecated Please use {@link SnsIO#write()} to avoid the risk of data loss.
   * @see <a href="https://github.com/apache/beam/issues/21366">Issue #21366</a>, <a
   *     href="https://issues.apache.org/jira/browse/BEAM-13203">BEAM-13203</a>
   */
  @Deprecated
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
