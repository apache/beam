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
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
import org.joda.time.Seconds;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for retractions support by {@link GroupByKey} in {@link DirectRunner}. */
public class GroupByKeyRetractionsTest implements Serializable {
  private static final DateTime START_TS = new DateTime(2017, 1, 1, 0, 0, 0, 0);
  private static final Boolean NOT_RETRACTION = false;
  private static final Boolean IS_RETRACTION = true;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Test pojo. */
  @AutoValue
  public abstract static class TestPojo implements Serializable {
    public abstract String k1();

    public abstract String k2();

    public abstract Instant eventTimestamp();

    public static TestPojo of(String k1, String k2, Instant eventTimestamp) {
      return new AutoValue_GroupByKeyRetractionsTest_TestPojo(k1, k2, eventTimestamp);
    }

    @Override
    public String toString() {
      return String.format("(%s_%s_%d)", k1(), k2(),
                           Seconds.secondsBetween(START_TS, eventTimestamp()).getSeconds());
    }
  }

  volatile Set<KV<KV<String, String>, Iterable<TestPojo>>> accumulator = new HashSet<>();

  @Test
  public void testMultipleGBKsAndCompositeKey() {
    PCollection<TestPojo> input =
        unboundedOf(
            TestPojo.of("K1", "K2", ts(8)),
            TestPojo.of("K1", "K2", ts(9)),
            TestPojo.of("K1", "K2", ts(10)),
            TestPojo.of("K1", "K2", ts(11)))
            .apply(
                Window.<TestPojo>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                    .accumulatingAndRetractingFiredPanes()
                    .withAllowedLateness(Duration.standardDays(365))
                    .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));

    PCollection<KV<Boolean, KV<KV<String, String>, Iterable<TestPojo>>>> output =
        input
            .apply("keyByK1", keyBy(TestPojo::k1))
            .setCoder(pojoKvCoder())
            .apply("gbK1", GroupByKey.create())
            .setCoder(
                KvCoder.of(
                    StringUtf8Coder.of(),
                    IterableCoder.of(SerializableCoder.of(TestPojo.class))))
            .apply("keyAlsoByK2", keyAlsoBy(TestPojo::k2))
            .setCoder(compositeKvCoder())
            .apply("gbK1AndK2", GroupByKey.create())
            .setCoder(compositeIterableKvCoder())
            .apply(
                "gbK1AndK2Output",
                retractionAwareParDo(
                    element -> KV.of(NOT_RETRACTION, element),
                    retraction -> KV.of(IS_RETRACTION, retraction)))
            .setCoder(KvCoder.of(BooleanCoder.of(), compositeIterableKvCoder()));

    PAssert
        .that(output)
        .containsInAnyOrder(
            KV.of(NOT_RETRACTION,
                  KV.of(
                      KV.of("K1", "K2"),
                      Arrays.asList(
                          TestPojo.of("K1", "K2", ts(8)),
                          TestPojo.of("K1", "K2", ts(9))))),

            KV.of(IS_RETRACTION,
                  KV.of(
                      KV.of("K1", "K2"),
                      Arrays.asList(
                          TestPojo.of("K1", "K2", ts(8)),
                          TestPojo.of("K1", "K2", ts(9))))),

            KV.of(NOT_RETRACTION,
                  KV.of(KV.of("K1", "K2"),
                        Arrays.asList(
                            TestPojo.of("K1", "K2", ts(8)),
                            TestPojo.of("K1", "K2", ts(9)),
                            TestPojo.of("K1", "K2", ts(8)),
                            TestPojo.of("K1", "K2", ts(9)),
                            TestPojo.of("K1", "K2", ts(8)),
                            TestPojo.of("K1", "K2", ts(9)),
                            TestPojo.of("K1", "K2", ts(10)),
                            TestPojo.of("K1", "K2", ts(11))))));
    pipeline.run();
  }

  @Test
  public void testRetractionsWithNestedKeys() {
    PCollection<TestPojo> input =
        unboundedOf(
            TestPojo.of("K1", "K2", ts(8)),
            TestPojo.of("K1", "K2", ts(9)),
            TestPojo.of("K1", "K2", ts(10)),
            TestPojo.of("K1", "K2", ts(11)))
            .apply(
                Window.<TestPojo>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(2)))
                    .accumulatingAndRetractingFiredPanes()
                    .withAllowedLateness(Duration.standardDays(365))
                    .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY));

    PCollection<KV<Boolean, KV<String, Iterable<KV<String, TestPojo>>>>> output =
        input
            .apply("nestedKeyByK1K2", nestedKeyByK1K2())
            .setCoder(nestedKvCoder())
            .apply("gbK1AndK2", GroupByKey.create())
            .setCoder(nestedIterableKvCoder())
            .apply(
                "gbK1AndK2Output",
                retractionAwareParDo(
                    element -> KV.of(NOT_RETRACTION, element),
                    retraction -> KV.of(IS_RETRACTION, retraction)))
            .setCoder(KvCoder.of(BooleanCoder.of(), nestedIterableKvCoder()));

    PAssert
        .that(output)
        .containsInAnyOrder(
            KV.of(
                NOT_RETRACTION,
                KV.of("K1",
                      Arrays.asList(
                          KV.of("K2", TestPojo.of("K1", "K2", ts(8))),
                          KV.of("K2", TestPojo.of("K1", "K2", ts(9)))))),

            KV.of(
                IS_RETRACTION,
                KV.of("K1",
                      Arrays.asList(
                          KV.of("K2", TestPojo.of("K1", "K2", ts(8))),
                          KV.of("K2", TestPojo.of("K1", "K2", ts(9)))))),

            KV.of(
                NOT_RETRACTION,
                KV.of("K1",
                      Arrays.asList(
                          KV.of("K2", TestPojo.of("K1", "K2", ts(8))),
                          KV.of("K2", TestPojo.of("K1", "K2", ts(9))),
                          KV.of("K2", TestPojo.of("K1", "K2", ts(10))),
                          KV.of("K2", TestPojo.of("K1", "K2", ts(11)))))));

    pipeline.run();
  }

  private Coder<KV<KV<String, String>, TestPojo>> compositeKvCoder() {
    return
        KvCoder.of(
            KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
            SerializableCoder.of(TestPojo.class));
  }

  private Coder<KV<KV<String, String>, Iterable<TestPojo>>> compositeIterableKvCoder() {
    return KvCoder.of(
        KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()),
        IterableCoder.of(SerializableCoder.of(TestPojo.class)));
  }

  private KvCoder<String, KV<String, TestPojo>> nestedKvCoder() {
    return
        KvCoder.of(
            StringUtf8Coder.of(),
            KvCoder.of(
                StringUtf8Coder.of(),
                SerializableCoder.of(TestPojo.class)));
  }

  private KvCoder<String, Iterable<KV<String, TestPojo>>> nestedIterableKvCoder() {
    return
        KvCoder.of(
            StringUtf8Coder.of(),
            IterableCoder.of(
                KvCoder.of(
                    StringUtf8Coder.of(),
                    SerializableCoder.of(TestPojo.class))));
  }

  private PCollection<TestPojo> unboundedOf(TestPojo... pojos) {
    TestStream.Builder<TestPojo> values = TestStream.create(SerializableCoder.of(TestPojo.class));

    for (TestPojo pet : pojos) {
      values = values.advanceWatermarkTo(pet.eventTimestamp());
      values = values.addElements(pet);
    }

    return PBegin.in(pipeline).apply("pojosUnbounded", values.advanceWatermarkToInfinity());
  }

  private Instant ts(int offset) {
    return START_TS.plusSeconds(offset).toInstant();
  }

  private Coder<KV<String, TestPojo>> pojoKvCoder() {
    return KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(TestPojo.class));
  }

  private static <InT, KeyT> PTransform<PCollection<? extends InT>, PCollection<KV<KeyT, InT>>>
  keyBy(SerializableFunction<InT, KeyT> func) {

    return ParDo.of(
        new DoFn<InT, KV<KeyT, InT>>() {
          @Pure
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(KV.of(func.apply(c.element()), c.element()));
          }
        });
  }

  private static PTransform<
      ? super PCollection<KV<String, Iterable<TestPojo>>>,
      PCollection<KV<KV<String, String>, TestPojo>>>
  keyAlsoBy(SerializableFunction<TestPojo, String> func) {

    return ParDo.of(
        new DoFn<KV<String, Iterable<TestPojo>>, KV<KV<String, String>, TestPojo>>() {
          @Pure
          @ProcessElement
          public void processElement(ProcessContext c) {
            KV<String, Iterable<TestPojo>> inKv = c.element();
            for (TestPojo elem : inKv.getValue()) {
              KV<String, String> newKey = KV.of(inKv.getKey(), func.apply(elem));
              KV<KV<String, String>, TestPojo> rekeyed = KV.of(newKey, elem);
              c.output(rekeyed);
            }
          }
        });
  }

  private PTransform<? super PCollection<TestPojo>, PCollection<KV<String, KV<String, TestPojo>>>>
  nestedKeyByK1K2() {

    return ParDo.of(
        new DoFn<TestPojo, KV<String, KV<String, TestPojo>>>() {
          @Pure
          @ProcessElement
          public void processElement(ProcessContext c) {
            TestPojo pojo = c.element();
            c.output(KV.of(pojo.k1(), KV.of(pojo.k2(), pojo)));
          }
        });
  }

  private static <InT, OutT> PTransform<PCollection<? extends InT>, PCollection<OutT>> pureParDo(
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

  private static <InT, OutT> PTransform<PCollection<? extends InT>, PCollection<OutT>>
  retractionAwareParDo(SerializableFunction<InT, OutT> processElementFunc,
                       SerializableFunction<InT, OutT> processRetractionFunc) {

    return ParDo.of(
        new DoFn<InT, OutT>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(processElementFunc.apply(c.element()));
          }

          @ProcessRetraction
          public void processRetraction(ProcessContext c) {
            c.output(processRetractionFunc.apply(c.element()));
          }
        });
  }
}
