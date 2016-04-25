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
package org.apache.beam.runners.dataflow.internal;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.base.Preconditions;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link CustomSources}.
 */
@RunWith(JUnit4.class)
public class CustomSourcesTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Rule public ExpectedLogs logged = ExpectedLogs.none(CustomSources.class);

  static class TestIO {
    public static Read fromRange(int from, int to) {
      return new Read(from, to, false);
    }

    static class Read extends BoundedSource<Integer> {
      final int from;
      final int to;
      final boolean produceTimestamps;

      Read(int from, int to, boolean produceTimestamps) {
        this.from = from;
        this.to = to;
        this.produceTimestamps = produceTimestamps;
      }

      public Read withTimestampsMillis() {
        return new Read(from, to, true);
      }

      @Override
      public List<Read> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options)
          throws Exception {
        List<Read> res = new ArrayList<>();
        DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
        float step = 1.0f * (to - from) / dataflowOptions.getNumWorkers();
        for (int i = 0; i < dataflowOptions.getNumWorkers(); ++i) {
          res.add(new Read(
              Math.round(from + i * step), Math.round(from + (i + 1) * step),
              produceTimestamps));
        }
        return res;
      }

      @Override
      public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
        return 8 * (to - from);
      }

      @Override
      public boolean producesSortedKeys(PipelineOptions options) throws Exception {
        return true;
      }

      @Override
      public BoundedReader<Integer> createReader(PipelineOptions options) throws IOException {
        return new RangeReader(this);
      }

      @Override
      public void validate() {}

      @Override
      public String toString() {
        return "[" + from + ", " + to + ")";
      }

      @Override
      public Coder<Integer> getDefaultOutputCoder() {
        return BigEndianIntegerCoder.of();
      }

      private static class RangeReader extends BoundedReader<Integer> {
        // To verify that BasicSerializableSourceFormat calls our methods according to protocol.
        enum State {
          UNSTARTED,
          STARTED,
          FINISHED
        }
        private Read source;
        private int current = -1;
        private State state = State.UNSTARTED;

        public RangeReader(Read source) {
          this.source = source;
        }

        @Override
        public boolean start() throws IOException {
          Preconditions.checkState(state == State.UNSTARTED);
          state = State.STARTED;
          current = source.from;
          return (current < source.to);
        }

        @Override
        public boolean advance() throws IOException {
          Preconditions.checkState(state == State.STARTED);
          if (current == source.to - 1) {
            state = State.FINISHED;
            return false;
          }
          current++;
          return true;
        }

        @Override
        public Integer getCurrent() {
          Preconditions.checkState(state == State.STARTED);
          return current;
        }

        @Override
        public Instant getCurrentTimestamp() {
          return source.produceTimestamps
              ? new Instant(current /* as millis */) : BoundedWindow.TIMESTAMP_MIN_VALUE;
        }

        @Override
        public void close() throws IOException {
          Preconditions.checkState(state == State.STARTED || state == State.FINISHED);
          state = State.FINISHED;
        }

        @Override
        public Read getCurrentSource() {
          return source;
        }

        @Override
        public Read splitAtFraction(double fraction) {
          int proposedIndex = (int) (source.from + fraction * (source.to - source.from));
          if (proposedIndex <= current) {
            return null;
          }
          Read primary = new Read(source.from, proposedIndex, source.produceTimestamps);
          Read residual = new Read(proposedIndex, source.to, source.produceTimestamps);
          this.source = primary;
          return residual;
        }

        @Override
        public Double getFractionConsumed() {
          return (current == -1)
              ? 0.0
              : (1.0 * (1 + current - source.from) / (source.to - source.from));
        }
      }
    }
  }

  @Test
  public void testDirectPipelineWithoutTimestamps() throws Exception {
    Pipeline p = TestPipeline.create();
    PCollection<Integer> sum = p
        .apply(Read.from(TestIO.fromRange(10, 20)))
        .apply(Sum.integersGlobally())
        .apply(Sample.<Integer>any(1));

    PAssert.thatSingleton(sum).isEqualTo(145);
    p.run();
  }

  @Test
  public void testDirectPipelineWithTimestamps() throws Exception {
    Pipeline p = TestPipeline.create();
    PCollection<Integer> sums =
        p.apply(Read.from(TestIO.fromRange(10, 20).withTimestampsMillis()))
         .apply(Window.<Integer>into(FixedWindows.of(Duration.millis(3))))
         .apply(Sum.integersGlobally().withoutDefaults());
    // Should group into [10 11] [12 13 14] [15 16 17] [18 19].
    PAssert.that(sums).containsInAnyOrder(21, 37, 39, 48);
    p.run();
  }

  @Test
  public void testRangeProgressAndSplitAtFraction() throws Exception {
    // Show basic usage of getFractionConsumed and splitAtFraction.
    // This test only tests TestIO itself, not BasicSerializableSourceFormat.

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    TestIO.Read source = TestIO.fromRange(10, 20);
    try (BoundedSource.BoundedReader<Integer> reader = source.createReader(options)) {
      assertEquals(0, reader.getFractionConsumed().intValue());
      assertTrue(reader.start());
      assertEquals(0.1, reader.getFractionConsumed(), 1e-6);
      assertTrue(reader.advance());
      assertEquals(0.2, reader.getFractionConsumed(), 1e-6);
      // Already past 0.0 and 0.1.
      assertNull(reader.splitAtFraction(0.0));
      assertNull(reader.splitAtFraction(0.1));

      {
        TestIO.Read residual = (TestIO.Read) reader.splitAtFraction(0.5);
        assertNotNull(residual);
        TestIO.Read primary = (TestIO.Read) reader.getCurrentSource();
        assertThat(readFromSource(primary, options), contains(10, 11, 12, 13, 14));
        assertThat(readFromSource(residual, options), contains(15, 16, 17, 18, 19));
      }

      // Range is now [10, 15) and we are at 12.
      {
        TestIO.Read residual = (TestIO.Read) reader.splitAtFraction(0.8); // give up 14.
        assertNotNull(residual);
        TestIO.Read primary = (TestIO.Read) reader.getCurrentSource();
        assertThat(readFromSource(primary, options), contains(10, 11, 12, 13));
        assertThat(readFromSource(residual, options), contains(14));
      }

      assertTrue(reader.advance());
      assertEquals(12, reader.getCurrent().intValue());
      assertTrue(reader.advance());
      assertEquals(13, reader.getCurrent().intValue());
      assertFalse(reader.advance());
    }
  }
}
