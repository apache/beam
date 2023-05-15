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
package org.apache.beam.sdk.io.aws2.sqs;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.net.URI;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * IO to read (unbounded) from and write to <a href="https://aws.amazon.com/sqs/">SQS</a> queues.
 *
 * <h3>Reading from SQS</h3>
 *
 * <p>{@link Read} returns an unbounded {@link PCollection} of {@link SqsMessage}s. As minimum
 * configuration you have to provide the {@code queue url} to connect to using {@link
 * Read#withQueueUrl(String)}.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<SqsMessage> output =
 *   pipeline.apply(SqsIO.read().withQueueUrl(queueUrl))
 * }</pre>
 *
 * <p>Note: Currently this source does not advance watermarks when no new messages are received.
 *
 * <h3>Writing to SQS</h3>
 *
 * <p>{@link Write} takes a {@link PCollection} of {@link SendMessageRequest}s as input. Each
 * request must contain the {@code queue url}. No further configuration is required.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * PCollection<SendMessageRequest> data = ...;
 * data.apply(SqsIO.write())
 * }</pre>
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
public class SqsIO {

  public static Read read() {
    return new AutoValue_SqsIO_Read.Builder()
        .setClientConfiguration(ClientConfiguration.builder().build())
        .setMaxNumRecords(Long.MAX_VALUE)
        .build();
  }

  public static Write write() {
    return new AutoValue_SqsIO_Write.Builder()
        .setClientConfiguration(ClientConfiguration.builder().build())
        .build();
  }

  private SqsIO() {}

  /**
   * A {@link PTransform} to read/receive messages from SQS. See {@link SqsIO} for more information
   * on usage and configuration.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<SqsMessage>> {

    abstract @Nullable ClientConfiguration clientConfiguration();

    abstract @Nullable String queueUrl();

    abstract long maxNumRecords();

    abstract @Nullable Duration maxReadTime();

    abstract @Nullable SqsClientProvider sqsClientProvider();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setClientConfiguration(ClientConfiguration config);

      abstract Builder setQueueUrl(String queueUrl);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Builder setSqsClientProvider(SqsClientProvider sqsClientProvider);

      abstract Read build();
    }

    /**
     * Define the max number of records received by the {@link Read}. When the max number of records
     * is lower than {@code Long.MAX_VALUE}, the {@link Read} will provide a bounded {@link
     * PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      return builder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages. When this
     * max read time is not null, the {@link Read} will provide a bounded {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      return builder().setMaxReadTime(maxReadTime).build();
    }

    /** Define the queueUrl used by the {@link Read} to receive messages from SQS. */
    public Read withQueueUrl(String queueUrl) {
      checkArgument(queueUrl != null, "queueUrl can not be null");
      checkArgument(!queueUrl.isEmpty(), "queueUrl can not be empty");
      return builder().setQueueUrl(queueUrl).build();
    }

    /**
     * @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. Alternatively
     *     you can configure a custom {@link ClientBuilderFactory} in {@link AwsOptions}.
     */
    @Deprecated
    public Read withSqsClientProvider(SqsClientProvider clientProvider) {
      checkArgument(clientProvider != null, "SqsClientProvider cannot be null");
      return builder().setClientConfiguration(null).setSqsClientProvider(clientProvider).build();
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Read withSqsClientProvider(AwsCredentialsProvider credentials, String region) {
      return updateClientConfig(
          b -> b.credentialsProvider(credentials).region(Region.of(region)).build());
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Read withSqsClientProvider(
        AwsCredentialsProvider credentials, String region, URI endpoint) {
      return updateClientConfig(
          b ->
              b.credentialsProvider(credentials)
                  .region(Region.of(region))
                  .endpoint(endpoint)
                  .build());
    }

    /** Configuration of SQS client. */
    public Read withClientConfiguration(ClientConfiguration config) {
      return updateClientConfig(ignore -> config);
    }

    private Read updateClientConfig(Function<ClientConfiguration.Builder, ClientConfiguration> fn) {
      checkState(
          sqsClientProvider() == null,
          "Legacy SqsClientProvider is set, but incompatible with ClientConfiguration.");
      ClientConfiguration config = fn.apply(clientConfiguration().toBuilder());
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return builder().setClientConfiguration(config).build();
    }

    @Override
    public PCollection<SqsMessage> expand(PBegin input) {
      if (clientConfiguration() != null) {
        AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
        ClientBuilderFactory.validate(awsOptions, clientConfiguration());
      }

      org.apache.beam.sdk.io.Read.Unbounded<SqsMessage> unbounded =
          org.apache.beam.sdk.io.Read.from(new SqsUnboundedSource(this));

      PTransform<PBegin, PCollection<SqsMessage>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }
  }
  // TODO: Add write batch api to improve performance
  /**
   * A {@link PTransform} to send messages to SQS. See {@link SqsIO} for more information on usage
   * and configuration.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<SendMessageRequest>, PDone> {

    abstract @Nullable ClientConfiguration getClientConfiguration();

    abstract @Nullable SqsClientProvider getSqsClientProvider();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setClientConfiguration(ClientConfiguration config);

      abstract Builder setSqsClientProvider(SqsClientProvider sqsClientProvider);

      abstract Write build();
    }

    /**
     * @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. Alternatively
     *     you can configure a custom {@link ClientBuilderFactory} in {@link AwsOptions}.
     */
    @Deprecated
    public Write withSqsClientProvider(SqsClientProvider clientProvider) {
      checkArgument(clientProvider != null, "SqsClientProvider cannot be null");
      return builder().setClientConfiguration(null).setSqsClientProvider(clientProvider).build();
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Write withSqsClientProvider(AwsCredentialsProvider credentials, String region) {
      return updateClientConfig(
          b -> b.credentialsProvider(credentials).region(Region.of(region)).build());
    }

    /** @deprecated Use {@link #withClientConfiguration(ClientConfiguration)} instead. */
    @Deprecated
    public Write withSqsClientProvider(
        AwsCredentialsProvider credentials, String region, URI endpoint) {
      return updateClientConfig(
          b ->
              b.credentialsProvider(credentials)
                  .region(Region.of(region))
                  .endpoint(endpoint)
                  .build());
    }

    /** Configuration of SQS client. */
    public Write withClientConfiguration(ClientConfiguration config) {
      return updateClientConfig(ignore -> config);
    }

    private Write updateClientConfig(
        Function<ClientConfiguration.Builder, ClientConfiguration> fn) {
      checkState(
          getSqsClientProvider() == null,
          "Legacy SqsClientProvider is set, but incompatible with ClientConfiguration.");
      ClientConfiguration config = fn.apply(getClientConfiguration().toBuilder());
      checkArgument(config != null, "ClientConfiguration cannot be null");
      return builder().setClientConfiguration(config).build();
    }

    @Override
    public PDone expand(PCollection<SendMessageRequest> input) {
      if (getSqsClientProvider() == null) {
        checkNotNull(getClientConfiguration(), "clientConfiguration cannot be null");
        AwsOptions awsOptions = input.getPipeline().getOptions().as(AwsOptions.class);
        ClientBuilderFactory.validate(awsOptions, getClientConfiguration());
      }

      input.apply(ParDo.of(new SqsWriteFn(this)));
      return PDone.in(input.getPipeline());
    }
  }

  private static class SqsWriteFn extends DoFn<SendMessageRequest, Void> {
    private final Write spec;
    private transient SqsClient sqs;

    SqsWriteFn(Write write) {
      this.spec = write;
    }

    @Setup
    public void setup(PipelineOptions options) throws Exception {
      if (spec.getSqsClientProvider() != null) {
        // build client using legacy SnsClientProvider
        sqs = spec.getSqsClientProvider().getSqsClient();
      } else {
        AwsOptions awsOpts = options.as(AwsOptions.class);
        sqs =
            ClientBuilderFactory.buildClient(
                awsOpts, SqsClient.builder(), spec.getClientConfiguration());
      }
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      sqs.sendMessage(processContext.element());
    }
  }
}
