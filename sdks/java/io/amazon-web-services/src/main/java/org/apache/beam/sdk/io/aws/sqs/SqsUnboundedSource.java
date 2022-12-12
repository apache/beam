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

import com.amazonaws.services.sqs.model.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws.sqs.SqsIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class SqsUnboundedSource extends UnboundedSource<Message, SqsCheckpointMark> {

  private final Read read;
  private final SqsConfiguration sqsConfiguration;
  private final Coder<Message> outputCoder;

  public SqsUnboundedSource(
      Read read, SqsConfiguration sqsConfiguration, Coder<Message> outputCoder) {
    this.read = read;
    this.sqsConfiguration = sqsConfiguration;
    this.outputCoder = outputCoder;
  }

  @Override
  public List<SqsUnboundedSource> split(int desiredNumSplits, PipelineOptions options) {
    List<SqsUnboundedSource> sources = new ArrayList<>();
    for (int i = 0; i < Math.max(1, desiredNumSplits); ++i) {
      sources.add(new SqsUnboundedSource(read, sqsConfiguration, outputCoder));
    }
    return sources;
  }

  @Override
  public UnboundedReader<Message> createReader(
      PipelineOptions options, @Nullable SqsCheckpointMark checkpointMark) {
    try {
      return new SqsUnboundedReader(this, checkpointMark);
    } catch (IOException e) {
      throw new RuntimeException("Unable to subscribe to " + read.queueUrl() + ": ", e);
    }
  }

  @Override
  public Coder<SqsCheckpointMark> getCheckpointMarkCoder() {
    return SerializableCoder.of(SqsCheckpointMark.class);
  }

  @Override
  public Coder<Message> getOutputCoder() {
    return outputCoder;
  }

  public Read getRead() {
    return read;
  }

  SqsConfiguration getSqsConfiguration() {
    return sqsConfiguration;
  }

  @Override
  public boolean requiresDeduping() {
    return true;
  }
}
