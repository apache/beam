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
package org.apache.beam.runners.spark.translation.streaming;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.SparkWatermarks;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.ElementEvent;
import org.apache.beam.sdk.testing.TestStream.ProcessingTimeEvent;
import org.apache.beam.sdk.testing.TestStream.WatermarkEvent;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.InputDStream;
import org.joda.time.Instant;
import scala.Option;
import scala.reflect.ClassTag;

public class TestDStream<T> extends InputDStream<WindowedValue<T>> {
  private final Coder<WindowedValue<T>> coder;

  @SuppressFBWarnings("SE_TRANSIENT_FIELD_NOT_RESTORED") // not intended for use after serialization
  private final transient @Nullable List<TestStream.Event<T>> events;

  private int currentEventIndex = 0;

  private boolean insertEmptyBatch = false;

  private long lastValidTimeMs = 0;

  private Instant lastWatermark = Instant.EPOCH;

  public TestDStream(TestStream<T> test, StreamingContext ssc) {
    super(ssc, classTag());
    this.coder = WindowedValue.getFullCoder(test.getValueCoder(), GlobalWindow.Coder.INSTANCE);
    this.events = test.getEvents();
  }

  @Override
  public Option<RDD<WindowedValue<T>>> compute(Time validTime) {
    TestStream.Event<T> event = insertEmptyBatch ? null : nextEvent();

    if (event == null) {
      insertEmptyBatch = false;
      waitForLastBatch(validTime);
      return Option.apply(emptyRDD());

    } else if (event instanceof ElementEvent) {
      waitForLastBatch(validTime);
      return Option.apply(buildRdd((ElementEvent<T>) event));

    } else if (event instanceof WatermarkEvent) {
      waitForLastBatch(validTime);
      addWatermark(validTime, (WatermarkEvent<T>) event);
      // insert an additional empty batch so watermark can propagate
      insertEmptyBatch = true;
      return Option.apply(emptyRDD());

    } else if (event instanceof ProcessingTimeEvent) {
      throw new UnsupportedOperationException(
          "Advancing Processing time is not supported by the Spark Runner.");
    } else {
      throw new IllegalStateException("Unknown event type " + event);
    }
  }

  private void waitForLastBatch(Time validTime) {
    while (GlobalWatermarkHolder.getLastWatermarkedBatchTime() < lastValidTimeMs) {
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    }
    lastValidTimeMs = validTime.milliseconds();
  }

  private @Nullable TestStream.Event<T> nextEvent() {
    List<TestStream.Event<T>> events = Preconditions.checkStateNotNull(this.events);
    return events.size() > currentEventIndex ? events.get(currentEventIndex++) : null;
  }

  private void addWatermark(Time time, WatermarkEvent<T> event) {
    SparkWatermarks watermarks =
        new SparkWatermarks(lastWatermark, event.getWatermark(), new Instant(time.milliseconds()));
    lastWatermark = event.getWatermark();
    GlobalWatermarkHolder.add(id(), watermarks);
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  private RDD<WindowedValue<T>> emptyRDD() {
    return ssc().sparkContext().emptyRDD(classTag());
  }

  private RDD<WindowedValue<T>> buildRdd(ElementEvent<T> event) {
    List<byte[]> binaryData = new ArrayList<>();
    for (TimestampedValue<T> elem : event.getElements()) {
      WindowedValue<T> wv =
          WindowedValue.timestampedValueInGlobalWindow(elem.getValue(), elem.getTimestamp());
      binaryData.add(CoderHelpers.toByteArray(wv, coder));
    }

    return new JavaSparkContext(ssc().sparkContext())
        .parallelize(binaryData)
        .map(CoderHelpers.fromByteFunction(coder))
        .rdd();
  }

  private static <T> ClassTag<WindowedValue<T>> classTag() {
    return JavaSparkContext$.MODULE$.fakeClassTag();
  }
}
