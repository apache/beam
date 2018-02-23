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
package org.apache.beam.runners.direct;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for retractions support by {@link ParDo} in {@link DirectRunner}. */
public class ParDoRetractionsTest {
  private static final DateTime START_TS = new DateTime(2017, 1, 1, 0, 0, 0, 0);

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  /** A test pojo. */
  @AutoValue
  public abstract static class TestPojo implements Serializable {
    public abstract Integer someIntKey();

    public abstract String someStringValue();

    public abstract Instant eventTimestamp();

    public static TestPojo of(Integer key, String value, Instant eventTimestamp) {
      return new AutoValue_ParDoRetractionsTest_TestPojo(key, value, eventTimestamp);
    }
  }

  @Test
  public void testElementsSeenMultipleTimesByPureParDo() {

    PCollection<TestPojo> input =
        unboundedOf(
            pojo(1, ts(0)),
            pojo(1, ts(1)),
            pojo(1, ts(2)),
            pojo(1, ts(3)),
            pojo(1, ts(7)))
            .apply(
                Window.<TestPojo>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                    .accumulatingAndRetractingFiredPanes()
                    .withAllowedLateness(Duration.standardDays(365))
                    .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));

    PCollection<List<TestPojo>> output =
        input
            .apply("assignKey", parDoOf(elem -> KV.of(elem.someIntKey(), elem)))
            .setCoder(pojoKvCoder())

            .apply("groupByKey", GroupByKey.create())

            .apply("emitEachGroupingAsList",
                   parDoOfPure(group -> (List<TestPojo>) Lists.newArrayList(group.getValue())))
            .setCoder(pojoListCoder());

    PAssert
        .that(output)
        .containsInAnyOrder(
            // first trigger firing
            Arrays.asList(pojo(1, ts(0)),
                          pojo(1, ts(1))),

            // second trigger firing. retraction of the previous output
            Arrays.asList(pojo(1, ts(0)),
                          pojo(1, ts(1))),

            // second trigger firing. added pojos with ts = 2 and 3
            Arrays.asList(pojo(1, ts(0)),
                          pojo(1, ts(1)),
                          pojo(1, ts(2)),
                          pojo(1, ts(3))),

            // last trigger firing. retraction of the previous output
            Arrays.asList(pojo(1, ts(0)),
                          pojo(1, ts(1)),
                          pojo(1, ts(2)),
                          pojo(1, ts(3))),

            // last trigger firing. added pojo with ts=7
            Arrays.asList(pojo(1, ts(0)),
                          pojo(1, ts(1)),
                          pojo(1, ts(2)),
                          pojo(1, ts(3)),
                          pojo(1, ts(7))));

    pipeline.run();
  }

  private PCollection<TestPojo> unboundedOf(TestPojo... pojos) {
    TestStream.Builder<TestPojo> values = TestStream.create(SerializableCoder.of(TestPojo.class));

    for (TestPojo pojo : pojos) {
      values = values.advanceWatermarkTo(pojo.eventTimestamp());
      // values = values.addElements(TimestampedValue.of(pojo, pojo.eventTimestamp()));
      values = values.addElements(pojo);
    }

    return PBegin.in(pipeline).apply("testPojosUnbounded", values.advanceWatermarkToInfinity());
  }

  private TestPojo pojo(int key, Instant timestamp) {
    return TestPojo.of(key, "pojo_at_" + timestamp.getMillis() + "_ms", timestamp);
  }

  private Instant ts(int offset) {
    return START_TS.plusSeconds(offset).toInstant();
  }

  private Coder<KV<Integer, TestPojo>> pojoKvCoder() {
    return KvCoder.of(VarIntCoder.of(), SerializableCoder.of(TestPojo.class));
  }

  private ListCoder<TestPojo> pojoListCoder() {
    return ListCoder.of(SerializableCoder.of(TestPojo.class));
  }

  private static <InT, OutT> PTransform<PCollection<? extends InT>, PCollection<OutT>> parDoOf(
      SerializableFunction<InT, OutT> func) {

    return ParDo.of(
        new DoFn<InT, OutT>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(func.apply(c.element()));
          }
        });
  }

  private static <InT, OutT> PTransform<PCollection<? extends InT>, PCollection<OutT>> parDoOfPure(
      SerializableFunction<InT, OutT> func) {

    return ParDo.of(
        new DoFn<InT, OutT>() {
          @Pure
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(func.apply(c.element()));
          }
        });
  }
}
