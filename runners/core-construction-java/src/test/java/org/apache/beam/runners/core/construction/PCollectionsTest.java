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

package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests for {@link PCollections}.
 */
@RunWith(Parameterized.class)
public class PCollectionsTest {
  // Each spec activates tests of all subsets of its fields
  @Parameters(name = "{index}: {0}")
  public static Iterable<PCollection<?>> data() {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> ints = pipeline.apply("ints", Create.of(1, 2, 3));
    PCollection<Long> longs = pipeline.apply("unbounded longs", GenerateSequence.from(0));
    PCollection<Long> windowedLongs =
        longs.apply(
            "into fixed windows",
            Window.<Long>into(FixedWindows.of(Duration.standardMinutes(10L))));
    PCollection<KV<String, Iterable<String>>> groupedStrings =
        pipeline
            .apply(
                "kvs", Create.of(KV.of("foo", "spam"), KV.of("bar", "ham"), KV.of("baz", "eggs")))
            .apply("group", GroupByKey.<String, String>create());
    PCollection<Long> coderLongs =
        pipeline
            .apply("counts with alternative coder", GenerateSequence.from(0).to(10))
            .setCoder(BigEndianLongCoder.of());
    PCollection<Integer> allCustomInts =
        pipeline
            .apply(
                "intsWithCustomCoder",
                Create.of(1, 2).withCoder(new AutoValue_PCollectionsTest_CustomIntCoder()))
            .apply(
                "into custom windows",
                Window.<Integer>into(new CustomWindows())
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(
                                AfterFirst.of(
                                    AfterPane.elementCountAtLeast(5),
                                    AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.millis(227L)))))
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.standardMinutes(12L)));
    return ImmutableList.<PCollection<?>>of(ints, longs, windowedLongs, coderLongs, groupedStrings);
  }

  @Parameter(0)
  public PCollection<?> testCollection;

  @Test
  public void testEncodeDecodeCycle() throws Exception {
    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.PCollection protoCollection = PCollections.toProto(testCollection, sdkComponents);
    RunnerApi.Components protoComponents = sdkComponents.toComponents();
    Coder<?> decodedCoder = PCollections.getCoder(protoCollection, protoComponents);
    WindowingStrategy<?, ?> decodedStrategy =
        PCollections.getWindowingStrategy(protoCollection, protoComponents);
    IsBounded decodedIsBounded = PCollections.isBounded(protoCollection);

    assertThat(decodedCoder, Matchers.<Coder<?>>equalTo(testCollection.getCoder()));
    assertThat(
        decodedStrategy,
        Matchers.<WindowingStrategy<?, ?>>equalTo(
            testCollection.getWindowingStrategy().fixDefaults()));
    assertThat(decodedIsBounded, equalTo(testCollection.isBounded()));
  }

  @AutoValue
  abstract static class CustomIntCoder extends CustomCoder<Integer> {
    @Override
    public void encode(Integer value, OutputStream outStream, Context context) throws IOException {
      VarInt.encode(value, outStream);
    }

    @Override
    public Integer decode(InputStream inStream, Context context) throws IOException {
      return VarInt.decodeInt(inStream);
    }
  }

  private static class CustomWindows extends NonMergingWindowFn<Integer, BoundedWindow> {
    @Override
    public Collection<BoundedWindow> assignWindows(final AssignContext c) throws Exception {
      return Collections.<BoundedWindow>singleton(
          new BoundedWindow() {
            @Override
            public Instant maxTimestamp() {
              return new Instant(c.element().longValue());
            }
          });
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other != null && this.getClass().equals(other.getClass());
    }

    @Override
    public Coder<BoundedWindow> windowCoder() {
      return new CustomCoder<BoundedWindow>() {
        @Override public void verifyDeterministic() {}

        @Override
        public void encode(BoundedWindow value, OutputStream outStream, Context context)
            throws IOException {
          VarInt.encode(value.maxTimestamp().getMillis(), outStream);
        }

        @Override
        public BoundedWindow decode(InputStream inStream, Context context) throws IOException {
          final Instant ts = new Instant(VarInt.decodeLong(inStream));
          return new BoundedWindow() {
            @Override
            public Instant maxTimestamp() {
              return ts;
            }
          };
        }
      };
    }

    @Override
    public WindowMappingFn<BoundedWindow> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException();
    }
  }
}
