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
package org.apache.beam.runners.samza.translation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;

/**
 * A Samza system factory that supports consuming from {@link TestStream} and translating events
 * into messages according to the {@link org.apache.beam.sdk.testing.TestStream.EventType} of the
 * events.
 */
public class SamzaTestStreamSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    final String streamPrefix = "systems." + systemName;
    final Config scopedConfig = config.subset(streamPrefix + ".", true);
    return new SmazaTestStreamSystemConsumer<>(getTestStream(scopedConfig));
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    throw new UnsupportedOperationException("SamzaTestStreamSystem doesn't support producing");
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SamzaTestStreamSystemAdmin();
  }

  /** A helper function to decode testStream from the config. */
  private static <T> TestStream<T> getTestStream(Config config) {
    @SuppressWarnings("unchecked")
    final SerializableFunction<String, TestStream<T>> testStreamDecoder =
        Base64Serializer.deserializeUnchecked(
            config.get("testStreamDecoder"), SerializableFunction.class);
    final TestStream<T> testStream = testStreamDecoder.apply(config.get("encodedTestStream"));
    return testStream;
  }

  private static final String DUMMY_OFFSET = "0";

  /** System admin for SmazaTestStreamSystem. */
  public static class SamzaTestStreamSystemAdmin implements SystemAdmin {
    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(
        Map<SystemStreamPartition, String> offsets) {
      return offsets.keySet().stream()
          .collect(Collectors.toMap(Function.identity(), k -> DUMMY_OFFSET));
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
      return streamNames.stream()
          .collect(
              Collectors.toMap(
                  Function.identity(),
                  stream -> {
                    // TestStream will always be single partition
                    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata>
                        partitionMetadata =
                            Collections.singletonMap(
                                new Partition(0),
                                new SystemStreamMetadata.SystemStreamPartitionMetadata(
                                    DUMMY_OFFSET, DUMMY_OFFSET, DUMMY_OFFSET));
                    return new SystemStreamMetadata(stream, partitionMetadata);
                  }));
    }

    @Override
    public Integer offsetComparator(String offset1, String offset2) {
      return 0;
    }
  }

  /** System consumer for SmazaTestStreamSystem. */
  public static class SmazaTestStreamSystemConsumer<T> implements SystemConsumer {
    TestStream<T> testStream;

    public SmazaTestStreamSystemConsumer(TestStream<T> testStream) {
      this.testStream = testStream;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String offset) {}

    @Override
    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
        Set<SystemStreamPartition> systemStreamPartitions, long timeout)
        throws InterruptedException {
      SystemStreamPartition ssp = systemStreamPartitions.iterator().next();
      ArrayList<IncomingMessageEnvelope> messages = new ArrayList<>();

      for (TestStream.Event<T> event : testStream.getEvents()) {
        if (event.getType().equals(TestStream.EventType.ELEMENT)) {
          // If event type is element, for each element, create a message with the element and
          // timestamp.
          for (TimestampedValue<T> element : ((TestStream.ElementEvent<T>) event).getElements()) {
            WindowedValue<T> windowedValue =
                WindowedValue.timestampedValueInGlobalWindow(
                    element.getValue(), element.getTimestamp());
            final OpMessage<T> opMessage = OpMessage.ofElement(windowedValue);
            final IncomingMessageEnvelope envelope =
                new IncomingMessageEnvelope(ssp, DUMMY_OFFSET, null, opMessage);
            messages.add(envelope);
          }
        } else if (event.getType().equals(TestStream.EventType.WATERMARK)) {
          // If event type is watermark, create a watermark message.
          long watermarkMillis = ((TestStream.WatermarkEvent<T>) event).getWatermark().getMillis();
          final IncomingMessageEnvelope envelope =
              IncomingMessageEnvelope.buildWatermarkEnvelope(ssp, watermarkMillis);
          messages.add(envelope);
          if (watermarkMillis == BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
            // If watermark reached max watermark, also create a end-of-stream message
            final IncomingMessageEnvelope endOfStreamMessage =
                IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp);
            messages.add(endOfStreamMessage);
            break;
          }
        } else if (event.getType().equals(TestStream.EventType.PROCESSING_TIME)) {
          throw new UnsupportedOperationException(
              "Advancing Processing time is not supported by the Samza Runner.");
        } else {
          throw new SamzaException("Unknown event type " + event.getType());
        }
      }

      return ImmutableMap.of(ssp, messages);
    }
  }
}
