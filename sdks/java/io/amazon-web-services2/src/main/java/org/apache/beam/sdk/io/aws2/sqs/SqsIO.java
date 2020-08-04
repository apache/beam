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

import com.google.auto.value.AutoValue;
import java.net.URI;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * An unbounded source for Amazon Simple Queue Service (SQS).
 *
 * <h3>Reading from an SQS queue</h3>
 *
 * <p>The {@link SqsIO} {@link Read} returns an unbounded {@link PCollection} of {@link
 * software.amazon.awssdk.services.sqs.model.Message} containing the received messages. Note: This
 * source does not currently advance the watermark when no new messages are received.
 *
 * <p>To configure an SQS source, you have to provide the queueUrl to connect to. The following
 * example illustrates how to configure the source:
 *
 * <pre>{@code
 * pipeline.apply(SqsIO.read().withQueueUrl(queueUrl))
 * }</pre>
 *
 * <h3>Writing to an SQS queue</h3>
 *
 * <p>The following example illustrates how to use the sink:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...) // returns PCollection<SendMessageRequest>
 *   .apply(SqsIO.write())
 * }</pre>
 *
 * <h3>Additional Configuration</h3>
 *
 * <p>Additional configuration can be provided via {@link AwsCredentialsProvider} in code. For
 * example, if you wanted to provide a secret access key via code:
 *
 * <pre>{@code
 * AwsCredentialsProvider provider = StaticCredentialsProvider.create(
 *    AwsBasicCredentials.create(ACCESS_KEY_ID, SECRET_ACCESS_KEY));
 * pipeline
 *   .apply(...) // returns PCollection<SendMessageRequest>
 *   .apply(SqsIO.write().withSqsClientProvider(provider))
 * }</pre>
 *
 * <p>For more information on the available options see {@link AwsOptions}.
 */
@Experimental(Kind.SOURCE_SINK)
public class SqsIO {

  public static Read read() {
    return new AutoValue_SqsIO_Read.Builder().setMaxNumRecords(Long.MAX_VALUE).build();
  }

  public static Write write() {
    return new AutoValue_SqsIO_Write.Builder().build();
  }

  private SqsIO() {}

  /**
   * A {@link PTransform} to read/receive messages from SQS. See {@link SqsIO} for more information
   * on usage and configuration.
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<SqsMessage>> {

    abstract @Nullable String queueUrl();

    abstract long maxNumRecords();

    abstract @Nullable Duration maxReadTime();

    abstract @Nullable SqsClientProvider sqsClientProvider();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
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
     * Allows to specify custom {@link SqsClientProvider}. {@link SqsClientProvider} creates new
     * {@link SqsClient} which is later used for writing to a SqS queue.
     */
    public Read withSqsClientProvider(SqsClientProvider awsClientsProvider) {
      return builder().setSqsClientProvider(awsClientsProvider).build();
    }

    /**
     * Specify {@link software.amazon.awssdk.auth.credentials.AwsCredentialsProvider} and region to
     * be used to read from SQS. If you need more sophisticated credential protocol, then you should
     * look at {@link Read#withSqsClientProvider(SqsClientProvider)}.
     */
    public Read withSqsClientProvider(AwsCredentialsProvider credentialsProvider, String region) {
      return withSqsClientProvider(credentialsProvider, region, null);
    }

    /**
     * Specify {@link AwsCredentialsProvider} and region to be used to write to SQS. If you need
     * more sophisticated credential protocol, then you should look at {@link
     * Read#withSqsClientProvider(SqsClientProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host. This is useful to execute
     * the tests with Kinesis service emulator.
     */
    public Read withSqsClientProvider(
        AwsCredentialsProvider credentialsProvider, String region, URI serviceEndpoint) {
      return withSqsClientProvider(
          new BasicSqsClientProvider(credentialsProvider, region, serviceEndpoint));
    }

    @Override
    public PCollection<SqsMessage> expand(PBegin input) {

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

    abstract @Nullable SqsClientProvider getSqsClientProvider();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSqsClientProvider(SqsClientProvider sqsClientProvider);

      abstract Write build();
    }

    /**
     * Allows to specify custom {@link SqsClientProvider}. {@link SqsClientProvider} creates new
     * {@link SqsClient} which is later used for writing to a SqS queue.
     */
    public Write withSqsClientProvider(SqsClientProvider awsClientsProvider) {
      return builder().setSqsClientProvider(awsClientsProvider).build();
    }

    /**
     * Specify {@link software.amazon.awssdk.auth.credentials.AwsCredentialsProvider} and region to
     * be used to write to SQS. If you need more sophisticated credential protocol, then you should
     * look at {@link Write#withSqsClientProvider(SqsClientProvider)}.
     */
    public Write withSqsClientProvider(AwsCredentialsProvider credentialsProvider, String region) {
      return withSqsClientProvider(credentialsProvider, region, null);
    }

    /**
     * Specify {@link AwsCredentialsProvider} and region to be used to write to SQS. If you need
     * more sophisticated credential protocol, then you should look at {@link
     * Write#withSqsClientProvider(SqsClientProvider)}.
     *
     * <p>The {@code serviceEndpoint} sets an alternative service host. This is useful to execute
     * the tests with Kinesis service emulator.
     */
    public Write withSqsClientProvider(
        AwsCredentialsProvider credentialsProvider, String region, URI serviceEndpoint) {
      return withSqsClientProvider(
          new BasicSqsClientProvider(credentialsProvider, region, serviceEndpoint));
    }

    @Override
    public PDone expand(PCollection<SendMessageRequest> input) {
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
    public void setup() {
      sqs = spec.getSqsClientProvider().getSqsClient();
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      sqs.sendMessage(processContext.element());
    }
  }
}
