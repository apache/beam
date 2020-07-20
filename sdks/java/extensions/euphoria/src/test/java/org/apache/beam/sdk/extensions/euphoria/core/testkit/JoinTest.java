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
package org.apache.beam.sdk.extensions.euphoria.core.testkit;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FullJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

/** Test operator {@code Join}. */
public class JoinTest extends AbstractOperatorTest {

  @Test
  public void batchJoinFullOuter() {
    execute(
        new JoinTestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            return FullJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Optional<Long> r, Collector<String> c) ->
                        c.collect(l.orElse(null) + "+" + r.orElse(null)))
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(0, "0+null"),
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(1, "1+11"),
                KV.of(1, "1+11"),
                KV.of(3, "3+13"),
                KV.of(3, "3+13"),
                KV.of(5, "null+15"));
          }
        });
  }

  @Test
  public void batchJoinFullOuterExample() {
    execute(
        new JoinTestCase<Integer, String, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<String> right) {

            return FullJoin.of(left, right)
                .by(le -> le, String::length)
                .using(
                    (Optional<Integer> l, Optional<String> r, Collector<String> c) ->
                        c.collect(l.orElse(null) + "+" + r.orElse(null)))
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 1);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<String> getRightInput() {
            return Arrays.asList("mouse", "rat", "cat", "X", "duck");
          }

          @Override
          protected TypeDescriptor<String> getRightInputType() {
            return TypeDescriptors.strings();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(1, "1+X"),
                KV.of(2, "2+null"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(0, "0+null"),
                KV.of(4, "4+duck"),
                KV.of(3, "3+cat"),
                KV.of(3, "3+rat"),
                KV.of(1, "1+X"),
                KV.of(5, "null+mouse"));
          }
        });
  }

  @Test
  public void batchJoinFullOuter_outputValues() {
    execute(
        new JoinTestCase<Integer, Long, String>() {

          @Override
          protected PCollection<String> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            return FullJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Optional<Long> r, Collector<String> c) ->
                        c.collect(l.orElse(null) + "+" + r.orElse(null)))
                .outputValues();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<String> getUnorderedOutput() {
            return Arrays.asList(
                "0+null", "2+12", "2+12", "4+14", "1+11", "1+11", "3+13", "3+13", "null+15");
          }
        });
  }

  @Test
  public void batchJoinLeftOuter() {
    execute(
        new JoinTestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            return LeftJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Integer l, Optional<Long> r, Collector<String> c) ->
                        c.collect(l + "+" + r.orElse(null)))
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(0, "0+null"),
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(1, "1+11"),
                KV.of(1, "1+11"),
                KV.of(3, "3+13"),
                KV.of(3, "3+13"));
          }
        });
  }

  @Test
  public void batchJoinRightOuter() {
    execute(
        new JoinTestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            return RightJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Long r, Collector<String> c) ->
                        c.collect(l.orElse(null) + "+" + r))
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(1, "1+11"),
                KV.of(1, "1+11"),
                KV.of(3, "3+13"),
                KV.of(3, "3+13"),
                KV.of(5, "null+15"));
          }
        });
  }

  @Test
  public void batchJoinInner() {
    execute(
        new JoinTestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            return Join.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Integer l, Long r, Collector<String> c) -> {
                      c.collect(l + "+" + r);
                    })
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(1, "1+11"),
                KV.of(1, "1+11"),
                KV.of(3, "3+13"),
                KV.of(3, "3+13"));
          }
        });
  }

  @Test
  public void windowJoinFullOuter() {
    execute(
        new JoinTestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            @SuppressWarnings("unchecked")
            final WindowFn<Object, BoundedWindow> evenOddWindowFn =
                (WindowFn) new EvenOddWindowFn();
            return FullJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Optional<Long> r, Collector<String> c) -> {
                      c.collect(l.orElse(null) + "+" + r.orElse(null));
                    })
                .windowBy(evenOddWindowFn)
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO)
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(0, "0+null"),
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(6, "6+16"),
                KV.of(8, "null+18"),
                KV.of(1, "1+null"),
                KV.of(1, "1+null"),
                KV.of(1, "null+11"),
                KV.of(3, "3+null"),
                KV.of(3, "3+null"),
                KV.of(3, "null+13"),
                KV.of(5, "5+null"),
                KV.of(5, "null+15"),
                KV.of(7, "null+17"));
          }
        });
  }

  @Test
  public void windowJoinLeftOuter() {
    execute(
        new JoinTestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            @SuppressWarnings("unchecked")
            final WindowFn<Object, BoundedWindow> evenOddWindowFn =
                (WindowFn) new EvenOddWindowFn();
            return LeftJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Integer l, Optional<Long> r, Collector<String> c) -> {
                      c.collect(l + "+" + r.orElse(null));
                    })
                .windowBy(evenOddWindowFn)
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO)
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(0, "0+null"),
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(6, "6+16"),
                KV.of(1, "1+null"),
                KV.of(1, "1+null"),
                KV.of(3, "3+null"),
                KV.of(3, "3+null"),
                KV.of(5, "5+null"));
          }
        });
  }

  @Test
  public void windowJoinRightOuter() {
    execute(
        new JoinTestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            @SuppressWarnings("unchecked")
            final WindowFn<Object, BoundedWindow> evenOddWindowFn =
                (WindowFn) new EvenOddWindowFn();
            return RightJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Long r, Collector<String> c) -> {
                      c.collect(l.orElse(null) + "+" + r);
                    })
                .windowBy(evenOddWindowFn)
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO)
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(4, "4+14"),
                KV.of(6, "6+16"),
                KV.of(8, "null+18"),
                KV.of(1, "null+11"),
                KV.of(3, "null+13"),
                KV.of(5, "null+15"),
                KV.of(7, "null+17"));
          }
        });
  }

  // ~ all of the inputs fall into the same session window (on the same key)
  // ~ we expect the result to reflect this fact
  // ~ note: no early triggering
  @Test
  public void joinOnSessionWindowingNoEarlyTriggering() {
    execute(
        new JoinTestCase<KV<String, Long>, KV<String, Long>, KV<String, String>>() {

          @Override
          protected List<KV<String, Long>> getLeftInput() {
            return Arrays.asList(KV.of("fi", 1L), KV.of("fa", 2L));
          }

          @Override
          protected TypeDescriptor<KV<String, Long>> getLeftInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs());
          }

          @Override
          protected List<KV<String, Long>> getRightInput() {
            return Arrays.asList(KV.of("ha", 1L), KV.of("ho", 4L));
          }

          @Override
          protected TypeDescriptor<KV<String, Long>> getRightInputType() {
            return TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs());
          }

          @Override
          protected PCollection<KV<String, String>> getOutput(
              PCollection<KV<String, Long>> left, PCollection<KV<String, Long>> right) {

            left =
                AssignEventTime.named("assign-event-time-left")
                    .of(left)
                    .using(KV::getValue)
                    .output();
            right =
                AssignEventTime.named("assign-event-time-right")
                    .of(right)
                    .using(KV::getValue)
                    .output();

            final PCollection<KV<String, KV<String, String>>> joined =
                Join.of(left, right)
                    .by(p -> "", p -> "")
                    .using(
                        (KV<String, Long> l, KV<String, Long> r, Collector<KV<String, String>> c) ->
                            c.collect(KV.of(l.getKey(), r.getKey())))
                    .windowBy(Sessions.withGapDuration(org.joda.time.Duration.millis(10)))
                    .triggeredBy(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO)
                    .output();

            return MapElements.of(joined).using(KV::getValue).output();
          }

          @Override
          public List<KV<String, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of("fi", "ha"), KV.of("fi", "ho"), KV.of("fa", "ha"), KV.of("fa", "ho"));
          }
        });
  }

  //  @Ignore(
  //      "This test is based on access to various objects through Environment which is "
  //          + "unsupported feature. It may be possible to add this feature in future.")
  //  @Test
  //  public void testJoinAccumulators() {
  //    execute(
  //        new JoinTestCase<
  //            KV<String, Long>, KV<String, Long>, Triple<TimeInterval, String, String>>() {
  //
  //          @Override
  //          protected List<KV<String, Long>> getLeftInput() {
  //            return Arrays.asList(KV.of("fi", 1L), KV.of("fa", 3L));
  //          }
  //
  //          @Override
  //          protected List<KV<String, Long>> getRightInput() {
  //            return Arrays.asList(KV.of("ha", 1L), KV.of("ho", 4L));
  //          }
  //
  //          @Override
  //          protected PCollection<Triple<TimeInterval, String, String>> getOutput(
  //              PCollection<KV<String, Long>> left, PCollection<KV<String, Long>> right) {
  //
  //            left = AssignEventTime.of(left).using(KV::getValue).output();
  //            right = AssignEventTime.of(right).using(KV::getValue).output();
  //
  //            PCollection<KV<String, Triple<TimeInterval, String, String>>> joined =
  //                Join.of(left, right)
  //                    .by(p -> "", p -> "")
  //                    .using(
  //                        (KV<String, Long> l,
  //                            KV<String, Long> r,
  //                            Collector<Triple<TimeInterval, String, String>> c) -> {
  //                          TimeInterval window = (TimeInterval) c.getWindow();
  //                          c.getCounter("cntr").increment(10);
  //                          c.getHistogram("hist-" + l.getKey().charAt(1)).add(2345, 8);
  //                          c.collect(Triple.of(window, l.getKey(), r.getKey()));
  //                        })
  //                    //                    .windowBy(Time.of(Duration.ofMillis(3)))
  //                    .windowBy(FixedWindows.of(org.joda.time.Duration.millis(3)))
  //                    .triggeredBy(AfterWatermark.pastEndOfWindow())
  //                    .discardingFiredPanes()
  //                    .output();
  //
  //            return MapElements.of(joined).using(KV::getValue).output();
  //          }
  //
  //          @Override
  //          public List<Triple<TimeInterval, String, String>> getUnorderedOutput() {
  //            return Arrays.asList(
  //                Triple.of(new TimeInterval(0, 3), "fi", "ha"),
  //                Triple.of(new TimeInterval(3, 6), "fa", "ho"));
  //          }
  //
  //          @Override
  //          public void validateAccumulators(SnapshotProvider snapshots) {
  //            Map<String, Long> counters = snapshots.getCounterSnapshots();
  //            assertEquals(Long.valueOf(20L), counters.get("cntr"));
  //
  //            Map<String, Map<Long, Long>> histograms = snapshots.getHistogramSnapshots();
  //            Map<Long, Long> hist = histograms.get("hist-i");
  //            assertEquals(1, hist.size());
  //            assertEquals(Long.valueOf(8), hist.get(2345L));
  //
  //            hist = histograms.get("hist-a");
  //            assertEquals(1, hist.size());
  //            assertEquals(Long.valueOf(8), hist.get(2345L));
  //          }
  //        });
  //  }

  @Test
  public void batchJoinFullOuterMultipleOutputsPerCollectorFunction() {
    execute(
        new JoinTestCase<Integer, Long, KV<Integer, String>>() {

          @Override
          protected PCollection<KV<Integer, String>> getOutput(
              PCollection<Integer> left, PCollection<Long> right) {
            return FullJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Optional<Long> r, Collector<String> c) -> {
                      // output everything twice
                      c.collect(l.orElse(null) + "+" + r.orElse(null));
                      c.collect(l.orElse(null) + "+" + r.orElse(null));
                    })
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(0, 1, 2, 3);
          }

          @Override
          protected TypeDescriptor<Integer> getLeftInputType() {
            return TypeDescriptors.integers();
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L);
          }

          @Override
          protected TypeDescriptor<Long> getRightInputType() {
            return TypeDescriptors.longs();
          }

          @Override
          public List<KV<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                KV.of(0, "0+null"),
                KV.of(0, "0+null"),
                KV.of(1, "1+11"),
                KV.of(1, "1+11"),
                KV.of(2, "2+12"),
                KV.of(2, "2+12"),
                KV.of(3, "3+13"),
                KV.of(3, "3+13"),
                KV.of(4, "null+14"),
                KV.of(4, "null+14"));
          }
        });
  }

  /** Base for join test cases. */
  public abstract static class JoinTestCase<LeftT, RightT, OutputT> implements TestCase<OutputT> {

    @Override
    public PCollection<OutputT> getOutput(Pipeline pipeline) {
      final PCollection<LeftT> left =
          pipeline
              .apply("left-input", Create.of(getLeftInput()))
              .setTypeDescriptor(getLeftInputType());
      final PCollection<RightT> right =
          pipeline
              .apply("right-input", Create.of(getRightInput()))
              .setTypeDescriptor(getRightInputType());
      return getOutput(left, right);
    }

    protected abstract PCollection<OutputT> getOutput(
        PCollection<LeftT> left, PCollection<RightT> right);

    protected abstract List<LeftT> getLeftInput();

    protected abstract TypeDescriptor<LeftT> getLeftInputType();

    protected abstract List<RightT> getRightInput();

    protected abstract TypeDescriptor<RightT> getRightInputType();
  }

  /**
   * Elements with even numeric values are are assigned to one 'even' window. All others are
   * assigned to window named 'win: #', where '#' is value of assigned element.
   */
  private static class EvenOddWindowFn extends WindowFn<KV<Integer, Number>, BoundedWindow> {

    private static final NamedGlobalWindow EVEN_WIN = new NamedGlobalWindow("even");

    @Override
    public Collection<BoundedWindow> assignWindows(AssignContext c) {
      final KV<Integer, Number> element = c.element();
      final Number value = element.getValue();
      if (value == null) {
        return Collections.singleton(EVEN_WIN);
      }
      final NamedGlobalWindow win;
      if (value.longValue() % 2 == 0) {
        win = EVEN_WIN;
      } else {
        win = new NamedGlobalWindow("win: " + value.longValue());
      }
      return Collections.singleton(win);
    }

    @Override
    public void mergeWindows(MergeContext c) {
      // no merging
    }

    /**
     * @param other
     * @return
     * @deprecated deprecated in super class
     */
    @Deprecated
    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof EvenOddWindowFn;
    }

    @Override
    public Coder<BoundedWindow> windowCoder() {
      return KryoCoder.of(PipelineOptionsFactory.create());
    }

    @Override
    public @Nullable WindowMappingFn<BoundedWindow> getDefaultWindowMappingFn() {
      return null;
    }

    @Override
    public boolean isNonMerging() {
      return true;
    }
  }

  private static class NamedGlobalWindow extends BoundedWindow {

    private String name;

    NamedGlobalWindow(String name) {
      this.name = name;
    }

    @Override
    public Instant maxTimestamp() {
      return GlobalWindow.INSTANCE.maxTimestamp();
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof NamedGlobalWindow) {
        return name.equals(((NamedGlobalWindow) other).name);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }
  }
}
