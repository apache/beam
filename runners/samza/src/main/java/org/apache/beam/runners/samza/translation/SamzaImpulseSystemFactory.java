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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.samza.Partition;
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
 * This is a trivial system for generating impulse event in Samza when translating IMPULSE transform
 * in portable api.
 */
public class SamzaImpulseSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(
      String systemName, Config config, MetricsRegistry metricsRegistry) {
    return new SamzaImpulseSystemConsumer();
  }

  @Override
  public SystemProducer getProducer(
      String systemName, Config config, MetricsRegistry metricsRegistry) {
    throw new UnsupportedOperationException("SamzaImpulseSystem doesn't support producing");
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SamzaImpulseSystemAdmin();
  }

  private static final String DUMMY_OFFSET = "0";

  /** System admin for ImpulseSystem. */
  public static class SamzaImpulseSystemAdmin implements SystemAdmin {
    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(
        Map<SystemStreamPartition, String> offset) {
      return offset.keySet().stream()
          .collect(Collectors.toMap(Function.identity(), k -> DUMMY_OFFSET));
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
      return streamNames.stream()
          .collect(
              Collectors.toMap(
                  Function.identity(),
                  stream -> {
                    // Impulse system will always be single partition
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

  /** System consumer for ImpulseSystem. */
  public static class SamzaImpulseSystemConsumer implements SystemConsumer {
    private AtomicBoolean isEnd = new AtomicBoolean(false);

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void register(SystemStreamPartition ssp, String offset) {}

    private static List<IncomingMessageEnvelope> constructMessages(SystemStreamPartition ssp) {
      final IncomingMessageEnvelope impulseMessage =
          new IncomingMessageEnvelope(
              ssp,
              DUMMY_OFFSET,
              /* key */ null,
              OpMessage.ofElement(WindowedValue.valueInGlobalWindow(new byte[0])));

      final IncomingMessageEnvelope watermarkMessage =
          IncomingMessageEnvelope.buildWatermarkEnvelope(
              ssp, BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());

      final IncomingMessageEnvelope endOfStreamMessage =
          IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp);

      return Arrays.asList(impulseMessage, watermarkMessage, endOfStreamMessage);
    }

    @Override
    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
        Set<SystemStreamPartition> ssps, long timeout) throws InterruptedException {
      if (isEnd.compareAndSet(false, true)) {
        return ssps.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(), SamzaImpulseSystemConsumer::constructMessages));
      } else {
        return Collections.emptyMap();
      }
    }
  }
}
