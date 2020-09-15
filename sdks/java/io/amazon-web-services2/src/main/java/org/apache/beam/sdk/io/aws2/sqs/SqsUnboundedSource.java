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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.aws2.sqs.SqsIO.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.services.sqs.SqsClient;

class SqsUnboundedSource extends UnboundedSource<SqsMessage, SqsCheckpointMark> {
  private final Read read;
  private final Supplier<SqsClient> sqs;

  public SqsUnboundedSource(Read read) {
    this.read = read;
    sqs =
        Suppliers.memoize(
            (Supplier<SqsClient> & Serializable) () -> read.sqsClientProvider().getSqsClient());
  }

  @Override
  public List<SqsUnboundedSource> split(int desiredNumSplits, PipelineOptions options) {
    List<SqsUnboundedSource> sources = new ArrayList<>();
    for (int i = 0; i < Math.max(1, desiredNumSplits); ++i) {
      sources.add(new SqsUnboundedSource(read));
    }
    return sources;
  }

  @Override
  public UnboundedReader<SqsMessage> createReader(
      PipelineOptions options, @Nullable SqsCheckpointMark checkpointMark) {
    return new SqsUnboundedReader(this, checkpointMark);
  }

  @Override
  public Coder<SqsCheckpointMark> getCheckpointMarkCoder() {
    return SerializableCoder.of(SqsCheckpointMark.class);
  }

  @Override
  public Coder<SqsMessage> getOutputCoder() {
    return SerializableCoder.of(SqsMessage.class);
  }

  public Read getRead() {
    return read;
  }

  public SqsClient getSqs() {
    return sqs.get();
  }

  @Override
  public boolean requiresDeduping() {
    return true;
  }
}
