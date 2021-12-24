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

import java.io.IOException;
import javax.naming.SizeLimitExceededException;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;

public class PreparePubsubWrite<InputT, DestinationT>
    extends PTransform<PCollection<InputT>, PCollection<PubsubMessage>> {
  protected PubsubDynamicDestinations<InputT, DestinationT> dynamicDestinations;
  protected SerializableFunction<InputT, PubsubMessage> formatFunction;
  // private Map<DestinationT, PubsubIO.PubsubTopic> destinationTopics;
  private ValueProvider<PubsubIO.PubsubTopic> pubsubTopicValueProvider;

  public PreparePubsubWrite(
      PubsubDynamicDestinations<InputT, DestinationT> dynamicDestinations,
      SerializableFunction<InputT, PubsubMessage> formatFunction,
      ValueProvider<PubsubIO.PubsubTopic> pubsubTopicValueProvider) {
    this.dynamicDestinations = dynamicDestinations;
    this.formatFunction = formatFunction;
    this.pubsubTopicValueProvider = pubsubTopicValueProvider;
  }

  @DoFn.StartBundle
  public void startBundle() throws IOException {
    // this.destinationTopics = Maps.newHashMap();
  }

  @Override
  public PCollection<PubsubMessage> expand(PCollection<InputT> input) {
    return input
        .apply(ParDo.of(new PreparePubsubWriteDoFn()))
        .setCoder(PubsubMessageWithAttributesAndMessageIdCoder.of());
  }

  public class PreparePubsubWriteDoFn extends DoFn<InputT, PubsubMessage> {
    @ProcessElement
    public void processElement(
        ProcessContext context,
        @Element InputT element,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        PaneInfo pane) {
      PubsubIO.PubsubTopic topic = getTopic(element, timestamp, window, pane);
      PubsubMessage outputValue = null;
      if (formatFunction != null) {
        outputValue = formatFunction.apply(element);
        checkArgument(
            outputValue != null,
            "formatFunction may not return null, but %s returned null on element %s",
            formatFunction,
            element);
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

    private PubsubIO.PubsubTopic getTopic(
        InputT element, Instant timestamp, BoundedWindow window, PaneInfo pane) {
      if (dynamicDestinations == null) {
        return pubsubTopicValueProvider.get();
      } else {
        ValueInSingleWindow<InputT> windowedElement =
            ValueInSingleWindow.of(element, timestamp, window, pane);
        DestinationT topicDestination = dynamicDestinations.getDestination(windowedElement);
        checkArgument(
            topicDestination != null,
            "DynamicDestinations.getDestination() may not return null, "
                + "but %s returned null on element %s",
            dynamicDestinations,
            element);
        PubsubIO.PubsubTopic topic = dynamicDestinations.getTopic(topicDestination);
      /* destinationTopics.computeIfAbsent(
      topicDestination, elem -> dynamicDestinations.getTopic(elem));*/
        return topic;
      }
    }
  }

  /*  private static class PreparePubsubWriteDoFn<InputT, DestinationT>
      extends DoFn<InputT, KV<PubsubIO.PubsubTopic, PubsubMessage>> {

    private PubsubDynamicDestinations<InputT, DestinationT> dynamicDestinations;
    private SerializableFunction<InputT, PubsubMessage> formatFunction;
    private ValueProvider<PubsubIO.PubsubTopic> pubsubTopicValueProvider;
    private Map<DestinationT, PubsubIO.PubsubTopic> destinationTopics;

    public PreparePubsubWriteDoFn(
        PubsubDynamicDestinations<InputT, DestinationT> dynamicDestinations,
        SerializableFunction<InputT, PubsubMessage> formatFunction,
        ValueProvider<PubsubIO.PubsubTopic> pubsubTopicValueProvider,
        Map<DestinationT, PubsubIO.PubsubTopic> destinationTopics) {
      this.dynamicDestinations = dynamicDestinations;
      this.formatFunction = formatFunction;
      this.pubsubTopicValueProvider = pubsubTopicValueProvider;
      this.destinationTopics = destinationTopics;
    }

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @Element InputT element,
        @Timestamp Instant timestamp,
        BoundedWindow window,
        PaneInfo pane) {

      PubsubIO.PubsubTopic topic = getTopic(element, timestamp, window, pane);
      PubsubMessage outputValue = formatFunction.apply(element);
      checkArgument(
          outputValue != null,
          "formatFunction may not return null, but %s returned null on element %s",
          formatFunction,
          element);
      try {
        validatePubsubMessage(outputValue);
      } catch (SizeLimitExceededException e) {
        throw new IllegalArgumentException(e);
      }
      context.output(KV.of(topic, outputValue));
    }

    private PubsubIO.PubsubTopic getTopic(
        InputT element, Instant timestamp, BoundedWindow window, PaneInfo pane) {
      if (this.dynamicDestinations == null) {
        return this.pubsubTopicValueProvider.get();
      } else {
        ValueInSingleWindow<InputT> windowedElement =
            ValueInSingleWindow.of(element, timestamp, window, pane);
        DestinationT topicDestination = dynamicDestinations.getDestination(windowedElement);
        PubsubIO.PubsubTopic topic =
            destinationTopics.computeIfAbsent(
                topicDestination, elem -> dynamicDestinations.getTopic(elem));
        checkArgument(
            topicDestination != null,
            "DynamicDestinations.getDestination() may not return null, "
                + "but %s returned null on element %s",
            dynamicDestinations,
            element);
        return topic;
      }
    }
  }*/
}
