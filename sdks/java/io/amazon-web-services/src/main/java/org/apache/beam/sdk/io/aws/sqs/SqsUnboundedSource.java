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

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws.sqs.SqsIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.checkerframework.checker.nullness.qual.Nullable;

class SqsUnboundedSource extends UnboundedSource<Message, SqsCheckpointMark> {

  private final Read read;
  private final SqsConfiguration sqsConfiguration;
  private final Supplier<AmazonSQS> sqs;

  public SqsUnboundedSource(Read read, SqsConfiguration sqsConfiguration) {
    this.read = read;
    this.sqsConfiguration = sqsConfiguration;

    sqs =
        Suppliers.memoize(
            (Supplier<AmazonSQS> & Serializable)
                () ->
                    AmazonSQSClientBuilder.standard()
                        .withClientConfiguration(sqsConfiguration.getClientConfiguration())
                        .withCredentials(sqsConfiguration.getAwsCredentialsProvider())
                        .withRegion(sqsConfiguration.getAwsRegion())
                        .build());
  }

  @Override
  public List<SqsUnboundedSource> split(int desiredNumSplits, PipelineOptions options) {
    List<SqsUnboundedSource> sources = new ArrayList<>();
    for (int i = 0; i < Math.max(1, desiredNumSplits); ++i) {
      sources.add(new SqsUnboundedSource(read, sqsConfiguration));
    }
    return sources;
  }

  @Override
  public UnboundedReader<Message> createReader(
      PipelineOptions options, @Nullable SqsCheckpointMark checkpointMark) {
    return new SqsUnboundedReader(this, checkpointMark);
  }

  @Override
  public Coder<SqsCheckpointMark> getCheckpointMarkCoder() {
    return SerializableCoder.of(SqsCheckpointMark.class);
  }

  @Override
  public Coder<Message> getOutputCoder() {
    return SerializableCoder.of(Message.class);
  }

  public Read getRead() {
    return read;
  }

  public AmazonSQS getSqs() {
    return sqs.get();
  }

  @Override
  public boolean requiresDeduping() {
    return true;
  }
}
