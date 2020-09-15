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
package org.apache.beam.sdk.io.aws.sqs;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * An unbounded source for Amazon Simple Queue Service (SQS).
 *
 * <h3>Reading from an SQS queue</h3>
 *
 * <p>The {@link SqsIO} {@link Read} returns an unbounded {@link PCollection} of {@link
 * com.amazonaws.services.sqs.model.Message} containing the received messages. Note: This source
 * does not currently advance the watermark when no new messages are received.
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
 * <p>Additional configuration can be provided via {@link AwsOptions} from command line args or in
 * code. For example, if you wanted to provide a secret access key via code:
 *
 * <pre>{@code
 * PipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().create();
 * AwsOptions awsOptions = pipelineOptions.as(AwsOptions.class);
 * BasicAWSCredentials awsCreds = new BasicAWSCredentials("accesskey", "secretkey");
 * awsOptions.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCreds));
 * Pipeline pipeline = Pipeline.create(options);
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
  public abstract static class Read extends PTransform<PBegin, PCollection<Message>> {

    abstract @Nullable String queueUrl();

    abstract long maxNumRecords();

    abstract @Nullable Duration maxReadTime();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setQueueUrl(String queueUrl);

      abstract Builder setMaxNumRecords(long maxNumRecords);

      abstract Builder setMaxReadTime(Duration maxReadTime);

      abstract Read build();
    }

    /**
     * Define the max number of records received by the {@link Read}. When the max number of records
     * is lower than {@code Long.MAX_VALUE}, the {@link Read} will provide a bounded {@link
     * PCollection}.
     */
    public Read withMaxNumRecords(long maxNumRecords) {
      return toBuilder().setMaxNumRecords(maxNumRecords).build();
    }

    /**
     * Define the max read time (duration) while the {@link Read} will receive messages. When this
     * max read time is not null, the {@link Read} will provide a bounded {@link PCollection}.
     */
    public Read withMaxReadTime(Duration maxReadTime) {
      return toBuilder().setMaxReadTime(maxReadTime).build();
    }

    /** Define the queueUrl used by the {@link Read} to receive messages from SQS. */
    public Read withQueueUrl(String queueUrl) {
      checkArgument(queueUrl != null, "queueUrl can not be null");
      checkArgument(!queueUrl.isEmpty(), "queueUrl can not be empty");
      return toBuilder().setQueueUrl(queueUrl).build();
    }

    @Override
    public PCollection<Message> expand(PBegin input) {

      org.apache.beam.sdk.io.Read.Unbounded<Message> unbounded =
          org.apache.beam.sdk.io.Read.from(
              new SqsUnboundedSource(
                  this,
                  new SqsConfiguration(input.getPipeline().getOptions().as(AwsOptions.class))));

      PTransform<PBegin, PCollection<Message>> transform = unbounded;

      if (maxNumRecords() < Long.MAX_VALUE || maxReadTime() != null) {
        transform = unbounded.withMaxReadTime(maxReadTime()).withMaxNumRecords(maxNumRecords());
      }

      return input.getPipeline().apply(transform);
    }
  }

  /**
   * A {@link PTransform} to send messages to SQS. See {@link SqsIO} for more information on usage
   * and configuration.
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<SendMessageRequest>, PDone> {
    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Write build();
    }

    @Override
    public PDone expand(PCollection<SendMessageRequest> input) {
      input.apply(
          ParDo.of(
              new SqsWriteFn(
                  new SqsConfiguration(input.getPipeline().getOptions().as(AwsOptions.class)))));
      return PDone.in(input.getPipeline());
    }
  }

  private static class SqsWriteFn extends DoFn<SendMessageRequest, Void> {
    private final SqsConfiguration sqsConfiguration;
    private transient AmazonSQS sqs;

    SqsWriteFn(SqsConfiguration sqsConfiguration) {
      this.sqsConfiguration = sqsConfiguration;
    }

    @Setup
    public void setup() {
      sqs =
          AmazonSQSClientBuilder.standard()
              .withClientConfiguration(sqsConfiguration.getClientConfiguration())
              .withCredentials(sqsConfiguration.getAwsCredentialsProvider())
              .withRegion(sqsConfiguration.getAwsRegion())
              .build();
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {
      sqs.sendMessage(processContext.element());
    }

    @Teardown
    public void teardown() throws Exception {
      if (sqs != null) {
        sqs.shutdown();
      }
    }
  }
}
