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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MultiStepCombine}. */
@RunWith(JUnit4.class)
public class MultiStepCombineTest implements Serializable {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private transient KvCoder<String, Long> combinedCoder =
      KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of());

  @Test
  public void testMultiStepCombine() {
    PCollection<KV<String, Long>> combined =
        pipeline
            .apply(
                Create.of(
                    KV.of("foo", 1L),
                    KV.of("bar", 2L),
                    KV.of("bizzle", 3L),
                    KV.of("bar", 4L),
                    KV.of("bizzle", 11L)))
            .apply(Combine.perKey(new MultiStepCombineFn()));

    PAssert.that(combined)
        .containsInAnyOrder(KV.of("foo", 1L), KV.of("bar", 6L), KV.of("bizzle", 14L));
    pipeline.run();
  }

  @Test
  public void testMultiStepCombineWindowed() {
    SlidingWindows windowFn = SlidingWindows.of(Duration.millis(6L)).every(Duration.millis(3L));
    PCollection<KV<String, Long>> combined =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("foo", 1L), new Instant(1L)),
                    TimestampedValue.of(KV.of("bar", 2L), new Instant(2L)),
                    TimestampedValue.of(KV.of("bizzle", 3L), new Instant(3L)),
                    TimestampedValue.of(KV.of("bar", 4L), new Instant(4L)),
                    TimestampedValue.of(KV.of("bizzle", 11L), new Instant(11L))))
            .apply(Window.into(windowFn))
            .apply(Combine.perKey(new MultiStepCombineFn()));

    PAssert.that("Windows should combine only elements in their windows", combined)
        .inWindow(new IntervalWindow(new Instant(0L), Duration.millis(6L)))
        .containsInAnyOrder(KV.of("foo", 1L), KV.of("bar", 6L), KV.of("bizzle", 3L));
    PAssert.that("Elements should appear in all the windows they are assigned to", combined)
        .inWindow(new IntervalWindow(new Instant(-3L), Duration.millis(6L)))
        .containsInAnyOrder(KV.of("foo", 1L), KV.of("bar", 2L));
    PAssert.that(combined)
        .inWindow(new IntervalWindow(new Instant(6L), Duration.millis(6L)))
        .containsInAnyOrder(KV.of("bizzle", 11L));
    PAssert.that(combined)
        .containsInAnyOrder(
            KV.of("foo", 1L),
            KV.of("foo", 1L),
            KV.of("bar", 6L),
            KV.of("bar", 2L),
            KV.of("bar", 4L),
            KV.of("bizzle", 11L),
            KV.of("bizzle", 11L),
            KV.of("bizzle", 3L),
            KV.of("bizzle", 3L));
    pipeline.run();
  }

  @Test
  public void testMultiStepCombineTimestampCombiner() {
    TimestampCombiner combiner = TimestampCombiner.LATEST;
    combinedCoder = KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of());
    PCollection<KV<String, Long>> combined =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(KV.of("foo", 4L), new Instant(1L)),
                    TimestampedValue.of(KV.of("foo", 1L), new Instant(4L)),
                    TimestampedValue.of(KV.of("bazzle", 4L), new Instant(4L)),
                    TimestampedValue.of(KV.of("foo", 12L), new Instant(12L))))
            .apply(
                Window.<KV<String, Long>>into(FixedWindows.of(Duration.millis(5L)))
                    .withTimestampCombiner(combiner))
            .apply(Combine.perKey(new MultiStepCombineFn()));
    PCollection<KV<String, TimestampedValue<Long>>> reified =
        combined.apply(
            ParDo.of(
                new DoFn<KV<String, Long>, KV<String, TimestampedValue<Long>>>() {
                  @ProcessElement
                  public void reifyTimestamp(ProcessContext context) {
                    context.output(
                        KV.of(
                            context.element().getKey(),
                            TimestampedValue.of(
                                context.element().getValue(), context.timestamp())));
                  }
                }));

    PAssert.that(reified)
        .containsInAnyOrder(
            KV.of("foo", TimestampedValue.of(5L, new Instant(4L))),
            KV.of("bazzle", TimestampedValue.of(4L, new Instant(4L))),
            KV.of("foo", TimestampedValue.of(12L, new Instant(12L))));
    pipeline.run();
  }

  private static class MultiStepCombineFn extends CombineFn<Long, MultiStepAccumulator, Long> {
    @Override
    public Coder<MultiStepAccumulator> getAccumulatorCoder(
        CoderRegistry registry, Coder<Long> inputCoder) throws CannotProvideCoderException {
      return new MultiStepAccumulatorCoder();
    }

    @Override
    public MultiStepAccumulator createAccumulator() {
      return MultiStepAccumulator.of(0L, false);
    }

    @Override
    public MultiStepAccumulator addInput(MultiStepAccumulator accumulator, Long input) {
      return MultiStepAccumulator.of(accumulator.getValue() + input, accumulator.isDeserialized());
    }

    @Override
    public MultiStepAccumulator mergeAccumulators(Iterable<MultiStepAccumulator> accumulators) {
      MultiStepAccumulator result = MultiStepAccumulator.of(0L, false);
      for (MultiStepAccumulator accumulator : accumulators) {
        result = result.merge(accumulator);
      }
      return result;
    }

    @Override
    public Long extractOutput(MultiStepAccumulator accumulator) {
      assertThat(
          "Accumulators should have been serialized and deserialized within the Pipeline",
          accumulator.isDeserialized(),
          is(true));
      return accumulator.getValue();
    }
  }

  @AutoValue
  abstract static class MultiStepAccumulator {
    private static MultiStepAccumulator of(long value, boolean deserialized) {
      return new AutoValue_MultiStepCombineTest_MultiStepAccumulator(value, deserialized);
    }

    MultiStepAccumulator merge(MultiStepAccumulator other) {
      return MultiStepAccumulator.of(
          this.getValue() + other.getValue(), this.isDeserialized() || other.isDeserialized());
    }

    abstract long getValue();

    abstract boolean isDeserialized();
  }

  private static class MultiStepAccumulatorCoder extends CustomCoder<MultiStepAccumulator> {
    @Override
    public void encode(MultiStepAccumulator value, OutputStream outStream)
        throws CoderException, IOException {
      VarInt.encode(value.getValue(), outStream);
    }

    @Override
    public MultiStepAccumulator decode(InputStream inStream) throws CoderException, IOException {
      return MultiStepAccumulator.of(VarInt.decodeLong(inStream), true);
    }
  }
}
