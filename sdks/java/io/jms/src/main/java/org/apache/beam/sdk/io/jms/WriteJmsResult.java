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
package org.apache.beam.sdk.io.jms;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Return type of {@link JmsIO.Write} transform. All messages in error are identified by: -
 * TupleTag<EventT> failedMessageTag - PCollection<EventT> failedMessages
 */
public class WriteJmsResult<EventT> implements POutput {

  private final Pipeline pipeline;
  private final TupleTag<EventT> failedMessageTag;
  private final PCollection<EventT> failedMessages;

  public WriteJmsResult(
      Pipeline pipeline, TupleTag<EventT> failedMessageTag, PCollection<EventT> failedMessages) {
    this.pipeline = pipeline;
    this.failedMessageTag = failedMessageTag;
    this.failedMessages = failedMessages;
  }

  static <FailevtT> WriteJmsResult<FailevtT> in(
      Pipeline pipeline,
      TupleTag<FailevtT> failedMessageTag,
      PCollection<FailevtT> failedMessages) {
    return new WriteJmsResult<FailevtT>(pipeline, failedMessageTag, failedMessages);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.of(failedMessageTag, failedMessages);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  public PCollection<EventT> getFailedMessages() {
    return failedMessages;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
