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
package org.apache.beam.sdk.io.aws2.sqs.providers;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.beam.sdk.io.aws2.sqs.SqsIO;
import org.apache.beam.sdk.io.aws2.sqs.SqsMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for jobs reading data from AWS SQS
 * queues and configured via {@link SqsReadConfiguration}.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@AutoService(SchemaTransformProvider.class)
public class SqsReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<SqsReadConfiguration> {

  public static final String OUTPUT_TAG = "output";

  public static final Schema OUTPUT_ROW_SCHEMA =
      Schema.builder()
          .addNullableStringField("body")
          .addNullableStringField("message_id")
          .addNullableStringField("receipt_handle")
          .addNullableInt64Field("timestamp")
          .addNullableInt64Field("request_timestamp")
          .build();

  @Override
  protected SchemaTransform from(SqsReadConfiguration configuration) {
    return new SqsReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:aws:sqs_read:v1";
  }

  @Override
  public String description() {
    return "Expose the SQS read functionality implemented in the Java SDK.";
  }

  @Override
  public List<String> outputCollectionNames() {
    return ImmutableList.of(OUTPUT_TAG);
  }

  private static class SqsReadSchemaTransform extends SchemaTransform {

    private final SqsReadConfiguration configuration;

    SqsReadSchemaTransform(SqsReadConfiguration config) {
      this.configuration = config;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      checkArgument(
          input.getAll().isEmpty(),
          String.format("Input to %s should be empty but it is not.", getClass().getSimpleName()));

      SqsIO.Read sqsRead =
          SqsIO.read()
              .withQueueUrl(configuration.getQueueUrl())
              .withMaxNumRecords(configuration.maxNumRecords());

      Long maxReadtimeSecs = configuration.getMaxReadTimeSecs();

      if (maxReadtimeSecs != null) {
        sqsRead = sqsRead.withMaxReadTime(Duration.standardSeconds(maxReadtimeSecs));
      }

      return PCollectionRowTuple.of(
          OUTPUT_TAG,
          input
              .getPipeline()
              .apply("ReadFromSqs", sqsRead)
              .apply(
                  "SqsMessageToRow",
                  MapElements.into(TypeDescriptors.rows()).via(new SqsMessageToBeamRow()))
              .setRowSchema(OUTPUT_ROW_SCHEMA));
    }
  }

  public static class SqsMessageToBeamRow implements SerializableFunction<SqsMessage, Row> {

    @Override
    public Row apply(SqsMessage input) {
      return Row.withSchema(OUTPUT_ROW_SCHEMA)
          .withFieldValue("body", input.getBody())
          .withFieldValue("message_id", input.getMessageId())
          .withFieldValue("receipt_handle", input.getReceiptHandle())
          .withFieldValue("timestamp", input.getTimeStamp())
          .withFieldValue("request_timestamp", input.getRequestTimeStamp())
          .build();
    }
  }
}
