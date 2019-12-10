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
package org.apache.beam.sdk.io.rabbitmq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

class RabbitMQSource extends UnboundedSource<RabbitMqMessage, RabbitMQCheckpointMark> {
  final RabbitMqIO.Read spec;

  RabbitMQSource(RabbitMqIO.Read spec) {
    this.spec = spec;
  }

  @Override
  public Coder<RabbitMqMessage> getOutputCoder() {
    return SerializableCoder.of(RabbitMqMessage.class);
  }

  @Override
  public List<RabbitMQSource> split(int desiredNumSplits, PipelineOptions options) {
    // RabbitMQ uses queue, so, we can have several concurrent consumers as source
    List<RabbitMQSource> sources = new ArrayList<>();
    for (int i = 0; i < desiredNumSplits; i++) {
      sources.add(this);
    }
    return sources;
  }

  @Override
  public UnboundedReader<RabbitMqMessage> createReader(
      PipelineOptions options, RabbitMQCheckpointMark checkpointMark) throws IOException {
    return new UnboundedRabbitMqReader(this, checkpointMark);
  }

  @Override
  public Coder<RabbitMQCheckpointMark> getCheckpointMarkCoder() {
    return SerializableCoder.of(RabbitMQCheckpointMark.class);
  }

  @Override
  public boolean requiresDeduping() {
    return true;
  }
}
