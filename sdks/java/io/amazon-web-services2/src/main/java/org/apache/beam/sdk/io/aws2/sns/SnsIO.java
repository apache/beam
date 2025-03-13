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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
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

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T>
      extends PTransform<PCollection<T>, PCollection<PublishResponse>> {

    abstract ClientConfiguration getClientConfiguration();

    abstract @Nullable String getTopicArn();

    abstract @Nullable SerializableFunction<T, PublishRequest.Builder> getPublishRequestBuilder();

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setClientConfiguration(ClientConfiguration config);

      abstract Builder<T> setTopicArn(String topicArn);

      abstract Builder<T> setPublishRequestBuilder(
          SerializableFunction<T, PublishRequest.Builder> requestBuilder);

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

    /** Configuration of SNS client. */
    public Write<T> withClientConfiguration(ClientConfiguration config) {
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return builder().setClientConfiguration(config).build();
    }

    @Override
    public PCollection<PublishResponse> expand(PCollection<T> input) {
      checkArgument(getPublishRequestBuilder() != null, "withPublishRequestBuilder() is required");

      AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
      checkArgument(getClientConfiguration() != null, "withClientConfiguration() is required");
      ClientBuilderFactory.validate(awsOptions, getClientConfiguration());
      if (getTopicArn() != null) {
        checkArgument(checkTopicExists(awsOptions), "Topic arn %s does not exist", getTopicArn());
      }

      return input.apply(ParDo.of(new SnsWriterFn<>(this)));
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
      return ClientBuilderFactory.buildClient(
          options.as(AwsOptions.class), SnsClient.builder(), getClientConfiguration());
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
}
