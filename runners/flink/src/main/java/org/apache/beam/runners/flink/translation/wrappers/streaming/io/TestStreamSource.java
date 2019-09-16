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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import java.util.List;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.Instant;

/** Flink source for executing {@link org.apache.beam.sdk.testing.TestStream}. */
public class TestStreamSource<T> extends RichSourceFunction<WindowedValue<T>> {

  private final SerializableFunction<byte[], TestStream<T>> testStreamDecoder;
  private final byte[] payload;

  private volatile boolean isRunning = true;

  public TestStreamSource(
      SerializableFunction<byte[], TestStream<T>> testStreamDecoder, byte[] payload) {
    this.testStreamDecoder = testStreamDecoder;
    this.payload = payload;
  }

  @Override
  public void run(SourceContext<WindowedValue<T>> ctx) throws CoderException {
    TestStream<T> testStream = testStreamDecoder.apply(payload);
    List<TestStream.Event<T>> events = testStream.getEvents();

    for (int eventId = 0; isRunning && eventId < events.size(); eventId++) {
      TestStream.Event<T> event = events.get(eventId);

      synchronized (ctx.getCheckpointLock()) {
        if (event instanceof TestStream.ElementEvent) {
          for (TimestampedValue<T> element : ((TestStream.ElementEvent<T>) event).getElements()) {
            Instant timestamp = element.getTimestamp();
            WindowedValue<T> value =
                WindowedValue.of(
                    element.getValue(), timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
            ctx.collectWithTimestamp(value, timestamp.getMillis());
          }
        } else if (event instanceof TestStream.WatermarkEvent) {
          long millis = ((TestStream.WatermarkEvent<T>) event).getWatermark().getMillis();
          ctx.emitWatermark(new Watermark(millis));
        } else if (event instanceof TestStream.ProcessingTimeEvent) {
          // There seems to be no clean way to implement this
          throw new UnsupportedOperationException(
              "Advancing Processing time is not supported by the Flink Runner.");
        } else {
          throw new IllegalStateException("Unknown event type " + event);
        }
      }
    }
  }

  @Override
  public void cancel() {
    this.isRunning = false;
  }
}
