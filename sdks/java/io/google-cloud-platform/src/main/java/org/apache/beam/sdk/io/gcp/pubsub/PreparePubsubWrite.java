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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.validatePubsubMessage;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import javax.naming.SizeLimitExceededException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class PreparePubsubWrite<InputT>
    extends PTransform<PCollection<InputT>, PCollection<PubsubMessage>> {
  private SerializableFunction<InputT, PubsubMessage> formatFunction;
  private ValueProvider<PubsubIO.PubsubTopic> topicValueProvider;

  public PreparePubsubWrite(
      ValueProvider<PubsubIO.PubsubTopic> topicValueProvider,
      SerializableFunction<InputT, PubsubMessage> formatFunction) {
    this.formatFunction = formatFunction;
    this.topicValueProvider = topicValueProvider;
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<InputT> input) {
    return input.apply(ParDo.of(new PreparePubsubWriteDoFn())).setCoder(PubsubMessageCoder.of());
  }

  public class PreparePubsubWriteDoFn extends DoFn<InputT, PubsubMessage> {
    @ProcessElement
    public void processElement(ProcessContext context, @Element InputT element) {
      PubsubMessage outputValue = null;
      if (formatFunction != null) {
        outputValue = formatFunction.apply(element);
        checkArgument(
            outputValue != null,
            "formatFunction may not return null, but %s returned null on element %s",
            formatFunction,
            element);

        if (outputValue.getTopicPath() == null && topicValueProvider.isAccessible()) {
          outputValue =
              new PubsubMessage(
                  outputValue.getPayload(),
                  outputValue.getAttributeMap(),
                  outputValue.getMessageId(),
                  topicValueProvider.get().asPath());
        }

      } else if (element.getClass().equals(PubsubMessage.class)) {
        outputValue = (PubsubMessage) element;
      }

      try {
        validatePubsubMessage(outputValue);
      } catch (SizeLimitExceededException e) {
        throw new IllegalArgumentException(e);
      }
      context.output(outputValue);
    }
  }
}
