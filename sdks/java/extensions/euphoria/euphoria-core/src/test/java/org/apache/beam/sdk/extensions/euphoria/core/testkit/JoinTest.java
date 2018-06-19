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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.TimeInterval;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FullJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SnapshotProvider;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.AbstractOperatorTest;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.junit.Processing;
import org.apache.beam.sdk.extensions.euphoria.core.translate.coder.KryoCoder;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;

/** Test operator {@code Join}. */
@Processing(Processing.Type.ALL)
public class JoinTest extends AbstractOperatorTest {

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinFullOuter() {
    execute(
        new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {
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
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(0, "0+null"),
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(1, "1+11"),
                Pair.of(1, "1+11"),
                Pair.of(3, "3+13"),
                Pair.of(3, "3+13"),
                Pair.of(5, "null+15"));
          }
        });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinFullOuter_outputValues() {
    execute(
        new JoinTestCase<Integer, Long, String>() {

          @Override
          protected Dataset<String> getOutput(Dataset<Integer> left, Dataset<Long> right) {
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
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          public List<String> getUnorderedOutput() {
            return Arrays.asList(
                "0+null", "2+12", "2+12", "4+14", "1+11", "1+11", "3+13", "3+13", "null+15");
          }
        });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinLeftOuter() {
    execute(
        new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {
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
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(0, "0+null"),
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(1, "1+11"),
                Pair.of(1, "1+11"),
                Pair.of(3, "3+13"),
                Pair.of(3, "3+13"));
          }
        });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinRightOuter() {
    execute(
        new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {
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
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(1, "1+11"),
                Pair.of(1, "1+11"),
                Pair.of(3, "3+13"),
                Pair.of(3, "3+13"),
                Pair.of(5, "null+15"));
          }
        });
  }

  @Processing(Processing.Type.BOUNDED)
  @Test
  public void batchJoinInner() {
    execute(
        new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {
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
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(1, "1+11"),
                Pair.of(1, "1+11"),
                Pair.of(3, "3+13"),
                Pair.of(3, "3+13"));
          }
        });
  }

  @Test
  public void windowJoinFullOuter() {
    execute(
        new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {

            WindowFn<Object, BoundedWindow> evenOddWindowFn = (WindowFn) new EvenOddWindowFn();

            return FullJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Optional<Long> r, Collector<String> c) -> {
                      c.collect(l.orElse(null) + "+" + r.orElse(null));
                    })
                .windowBy(evenOddWindowFn)
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(0, "0+null"),
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(6, "6+16"),
                Pair.of(8, "null+18"),
                Pair.of(1, "1+null"),
                Pair.of(1, "1+null"),
                Pair.of(1, "null+11"),
                Pair.of(3, "3+null"),
                Pair.of(3, "3+null"),
                Pair.of(3, "null+13"),
                Pair.of(5, "5+null"),
                Pair.of(5, "null+15"),
                Pair.of(7, "null+17"));
          }
        });
  }

  @Test
  public void windowJoinLeftOuter() {
    execute(
        new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {

            WindowFn<Object, BoundedWindow> evenOddWindowFn = (WindowFn) new EvenOddWindowFn();

            return LeftJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Integer l, Optional<Long> r, Collector<String> c) -> {
                      c.collect(l + "+" + r.orElse(null));
                    })
                .windowBy(evenOddWindowFn)
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(0, "0+null"),
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(6, "6+16"),
                Pair.of(1, "1+null"),
                Pair.of(1, "1+null"),
                Pair.of(3, "3+null"),
                Pair.of(3, "3+null"),
                Pair.of(5, "5+null"));
          }
        });
  }

  @Test
  public void windowJoinRightOuter() {
    execute(
        new JoinTestCase<Integer, Long, Pair<Integer, String>>() {

          @Override
          protected Dataset<Pair<Integer, String>> getOutput(
              Dataset<Integer> left, Dataset<Long> right) {

            WindowFn<Object, BoundedWindow> evenOddWindowFn = (WindowFn) new EvenOddWindowFn();

            return RightJoin.of(left, right)
                .by(e -> e, e -> (int) (e % 10))
                .using(
                    (Optional<Integer> l, Long r, Collector<String> c) -> {
                      c.collect(l.orElse(null) + "+" + r);
                    })
                .windowBy(evenOddWindowFn)
                .triggeredBy(AfterWatermark.pastEndOfWindow())
                .discardingFiredPanes()
                .output();
          }

          @Override
          protected List<Integer> getLeftInput() {
            return Arrays.asList(1, 2, 3, 0, 4, 3, 2, 1, 5, 6);
          }

          @Override
          protected List<Long> getRightInput() {
            return Arrays.asList(11L, 12L, 13L, 14L, 15L, 16L, 17L, 18L);
          }

          @Override
          public List<Pair<Integer, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of(2, "2+12"),
                Pair.of(2, "2+12"),
                Pair.of(4, "4+14"),
                Pair.of(6, "6+16"),
                Pair.of(8, "null+18"),
                Pair.of(1, "null+11"),
                Pair.of(3, "null+13"),
                Pair.of(5, "null+15"),
                Pair.of(7, "null+17"));
          }
        });
  }

  // ~ all of the inputs fall into the same session window (on the same key)
  // ~ we expect the result to reflect this fact
  // ~ note: no early triggering
  @Test
  public void joinOnSessionWindowingNoEarlyTriggering() {
    execute(
        new JoinTestCase<Pair<String, Long>, Pair<String, Long>, Pair<String, String>>() {

          @Override
          protected List<Pair<String, Long>> getLeftInput() {
            return Arrays.asList(Pair.of("fi", 1L), Pair.of("fa", 2L));
          }

          @Override
          protected List<Pair<String, Long>> getRightInput() {
            return Arrays.asList(Pair.of("ha", 1L), Pair.of("ho", 4L));
          }

          @Override
          protected Dataset<Pair<String, String>> getOutput(
              Dataset<Pair<String, Long>> left, Dataset<Pair<String, Long>> right) {

            left = AssignEventTime.of(left).using(Pair::getSecond).output();
            right = AssignEventTime.of(right).using(Pair::getSecond).output();

            Dataset<Pair<String, Pair<String, String>>> joined =
                Join.of(left, right)
                    .by(p -> "", p -> "")
                    .using(
                        (Pair<String, Long> l,
                            Pair<String, Long> r,
                            Collector<Pair<String, String>> c) ->
                            c.collect(Pair.of(l.getFirst(), r.getFirst())))
                    .windowBy(Sessions.withGapDuration(org.joda.time.Duration.millis(10)))
                    .triggeredBy(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .output();

            return MapElements.of(joined).using(Pair::getSecond).output();
          }

          @Override
          public List<Pair<String, String>> getUnorderedOutput() {
            return Arrays.asList(
                Pair.of("fi", "ha"), Pair.of("fi", "ho"), Pair.of("fa", "ha"), Pair.of("fa", "ho"));
          }
        });
  }

  @Ignore(
      "This test is based on access to various objects through Environment which is "
          + "unsupported feature. It may be possible to add this feature in future.")
  @Test
  public void testJoinAccumulators() {
    execute(
        new JoinTestCase<
            Pair<String, Long>, Pair<String, Long>, Triple<TimeInterval, String, String>>() {

          @Override
          protected List<Pair<String, Long>> getLeftInput() {
            return Arrays.asList(Pair.of("fi", 1L), Pair.of("fa", 3L));
          }

          @Override
          protected List<Pair<String, Long>> getRightInput() {
            return Arrays.asList(Pair.of("ha", 1L), Pair.of("ho", 4L));
          }

          @Override
          protected Dataset<Triple<TimeInterval, String, String>> getOutput(
              Dataset<Pair<String, Long>> left, Dataset<Pair<String, Long>> right) {

            left = AssignEventTime.of(left).using(Pair::getSecond).output();
            right = AssignEventTime.of(right).using(Pair::getSecond).output();

            Dataset<Pair<String, Triple<TimeInterval, String, String>>> joined =
                Join.of(left, right)
                    .by(p -> "", p -> "")
                    .using(
                        (Pair<String, Long> l,
                            Pair<String, Long> r,
                            Collector<Triple<TimeInterval, String, String>> c) -> {
                          TimeInterval window = (TimeInterval) c.getWindow();
                          c.getCounter("cntr").increment(10);
                          c.getHistogram("hist-" + l.getFirst().charAt(1)).add(2345, 8);
                          c.collect(Triple.of(window, l.getFirst(), r.getFirst()));
                        })
                    //                    .windowBy(Time.of(Duration.ofMillis(3)))
                    .windowBy(FixedWindows.of(org.joda.time.Duration.millis(3)))
                    .triggeredBy(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .output();

            return MapElements.of(joined).using(Pair::getSecond).output();
          }

          @Override
          public List<Triple<TimeInterval, String, String>> getUnorderedOutput() {
            return Arrays.asList(
                Triple.of(new TimeInterval(0, 3), "fi", "ha"),
                Triple.of(new TimeInterval(3, 6), "fa", "ho"));
          }

          @Override
          public void validateAccumulators(SnapshotProvider snapshots) {
            Map<String, Long> counters = snapshots.getCounterSnapshots();
            assertEquals(Long.valueOf(20L), counters.get("cntr"));

            Map<String, Map<Long, Long>> histograms = snapshots.getHistogramSnapshots();
            Map<Long, Long> hist = histograms.get("hist-i");
            assertEquals(1, hist.size());
            assertEquals(Long.valueOf(8), hist.get(2345L));

            hist = histograms.get("hist-a");
            assertEquals(1, hist.size());
            assertEquals(Long.valueOf(8), hist.get(2345L));
          }
        });
  }

  /** Base for join test cases. */
  public abstract static class JoinTestCase<LeftT, RightT, OutputT> implements TestCase<OutputT> {

    @Override
    public Dataset<OutputT> getOutput(Flow flow, boolean bounded) {
      Dataset<LeftT> left = flow.createInput(ListDataSource.of(bounded, getLeftInput()));
      Dataset<RightT> right = flow.createInput(ListDataSource.of(bounded, getRightInput()));
      return getOutput(left, right);
    }

    protected abstract Dataset<OutputT> getOutput(Dataset<LeftT> left, Dataset<RightT> right);

    protected abstract List<LeftT> getLeftInput();

    protected abstract List<RightT> getRightInput();
  }

  /**
   * Elements with even numeric values are are assigned to one 'even' window. All others are
   * assigned to window named 'win: #', where '#' is value of assigned element.
   */
  private static class EvenOddWindowFn extends WindowFn<KV<Integer, Number>, BoundedWindow> {

    private static final NamedGlobalWindow EVEN_WIN = new NamedGlobalWindow("even");

    @Override
    public Collection<BoundedWindow> assignWindows(AssignContext c) throws Exception {
      KV<Integer, Number> element = c.element();

      Number value = element.getValue();

      if (value == null) {
        return Collections.singleton(EVEN_WIN);
      }

      NamedGlobalWindow win;
      if (value.longValue() % 2 == 0) {
        win = EVEN_WIN;
      } else {
        win = new NamedGlobalWindow("win: " + value.longValue());
      }

      return Collections.singleton(win);
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
      // no merging
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof EvenOddWindowFn;
    }

    @Override
    public Coder<BoundedWindow> windowCoder() {
      return new KryoCoder<>();
    }

    @Override
    @Nullable
    public WindowMappingFn<BoundedWindow> getDefaultWindowMappingFn() {
      return null;
    }

    @Override
    public boolean isNonMerging() {
      return true;
    }
  }

  private static class NamedGlobalWindow extends BoundedWindow {

    private String name;

    public NamedGlobalWindow(String name) {
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
