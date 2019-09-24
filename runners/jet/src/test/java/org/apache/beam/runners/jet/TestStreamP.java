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
package org.apache.beam.runners.jet;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.ThrottleWrappedP;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's {@link TestStream}
 * transform.
 */
public class TestStreamP extends AbstractProcessor {

  private final Traverser traverser;

  @SuppressWarnings("unchecked")
  private TestStreamP(byte[] payload, TestStream.TestStreamCoder payloadCoder, Coder outputCoder) {
    List events = decodePayload(payload, payloadCoder).getEvents();
    traverser =
        Traversers.traverseStream(
            events.stream()
                .flatMap(
                    event -> {
                      if (event instanceof TestStream.WatermarkEvent) {
                        Instant watermark = ((TestStream.WatermarkEvent) event).getWatermark();
                        if (BoundedWindow.TIMESTAMP_MAX_VALUE.equals(watermark)) {
                          // this is an element added by advanceWatermarkToInfinity(), we ignore it,
                          // it's always at the end
                          return null;
                        }
                        return Stream.of(new Watermark(watermark.getMillis()));
                      } else if (event instanceof TestStream.ElementEvent) {
                        return StreamSupport.stream(
                                ((TestStream.ElementEvent<?>) event).getElements().spliterator(),
                                false)
                            .map(
                                tv ->
                                    WindowedValue.timestampedValueInGlobalWindow(
                                        tv.getValue(), tv.getTimestamp()))
                            .map(wV -> Utils.encode(wV, outputCoder));
                      } else {
                        throw new UnsupportedOperationException(
                            "Event type not supported in TestStream: "
                                + event.getClass()
                                + ", event: "
                                + event);
                      }
                    }));
  }

  public static <T> ProcessorMetaSupplier supplier(
      byte[] payload, TestStream.TestStreamCoder payloadCoder, Coder outputCoder) {
    return ProcessorMetaSupplier.forceTotalParallelismOne(
        ProcessorSupplier.of(
            () -> new ThrottleWrappedP(new TestStreamP(payload, payloadCoder, outputCoder), 4)));
  }

  private static TestStream decodePayload(byte[] payload, TestStream.TestStreamCoder coder) {
    try {
      return (TestStream) CoderUtils.decodeFromByteArray(coder, payload);
    } catch (CoderException e) {
      throw ExceptionUtil.rethrow(e);
    }
  }

  @Override
  public boolean complete() {
    return emitFromTraverser(traverser);
  }
}
