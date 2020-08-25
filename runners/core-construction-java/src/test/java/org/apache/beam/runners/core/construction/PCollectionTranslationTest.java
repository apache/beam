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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
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
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link PCollectionTranslation}. */
@RunWith(Parameterized.class)
public class PCollectionTranslationTest {
  // Each spec activates tests of all subsets of its fields
  @Parameters(name = "{index}: {0}")
  public static Iterable<PCollection<?>> data() {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> ints = pipeline.apply("ints", Create.of(1, 2, 3));
    PCollection<Long> longs = pipeline.apply("unbounded longs", GenerateSequence.from(0));
    PCollection<Long> windowedLongs =
        longs.apply(
            "into fixed windows", Window.into(FixedWindows.of(Duration.standardMinutes(10L))));
    PCollection<KV<String, Iterable<String>>> groupedStrings =
        pipeline
            .apply(
                "kvs", Create.of(KV.of("foo", "spam"), KV.of("bar", "ham"), KV.of("baz", "eggs")))
            .apply("group", GroupByKey.create());
    PCollection<Long> coderLongs =
        pipeline
            .apply("counts with alternative coder", GenerateSequence.from(0).to(10))
            .setCoder(BigEndianLongCoder.of());
    pipeline
        .apply(
            "intsWithCustomCoder",
            Create.of(1, 2).withCoder(new AutoValue_PCollectionTranslationTest_CustomIntCoder()))
        .apply(
            "into custom windows",
            Window.into(new CustomWindows())
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(
                            AfterFirst.of(
                                AfterPane.elementCountAtLeast(5),
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(Duration.millis(227L)))))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.standardMinutes(12L)));
    return ImmutableList.of(ints, longs, windowedLongs, coderLongs, groupedStrings);
  }

  @Parameter(0)
  public PCollection<?> testCollection;

  @Test
  public void testEncodeDecodeCycle() throws Exception {
    // Encode
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.PCollection protoCollection =
        PCollectionTranslation.toProto(testCollection, sdkComponents);
    RehydratedComponents protoComponents =
        RehydratedComponents.forComponents(sdkComponents.toComponents());

    // Decode
    Pipeline pipeline = Pipeline.create();
    PCollection<?> decodedCollection =
        PCollectionTranslation.fromProto(protoCollection, pipeline, protoComponents);

    // Verify
    assertThat(decodedCollection.getCoder(), equalTo(testCollection.getCoder()));
    assertThat(
        decodedCollection.getWindowingStrategy(),
        equalTo(
            testCollection
                .getWindowingStrategy()
                .withEnvironmentId(sdkComponents.getOnlyEnvironmentId())
                .fixDefaults()));
    assertThat(decodedCollection.isBounded(), equalTo(testCollection.isBounded()));
  }

  @Test
  public void testEncodeDecodeFields() throws Exception {
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.PCollection protoCollection =
        PCollectionTranslation.toProto(testCollection, sdkComponents);
    RehydratedComponents protoComponents =
        RehydratedComponents.forComponents(sdkComponents.toComponents());
    Coder<?> decodedCoder = protoComponents.getCoder(protoCollection.getCoderId());
    WindowingStrategy<?, ?> decodedStrategy =
        protoComponents.getWindowingStrategy(protoCollection.getWindowingStrategyId());
    IsBounded decodedIsBounded = PCollectionTranslation.isBounded(protoCollection);

    assertThat(decodedCoder, equalTo(testCollection.getCoder()));
    assertThat(
        decodedStrategy,
        equalTo(
            testCollection
                .getWindowingStrategy()
                .withEnvironmentId(sdkComponents.getOnlyEnvironmentId())
                .fixDefaults()));
    assertThat(decodedIsBounded, equalTo(testCollection.isBounded()));
  }

  @AutoValue
  abstract static class CustomIntCoder extends CustomCoder<Integer> {
    @Override
    public Integer decode(InputStream inStream) throws IOException {
      return VarInt.decodeInt(inStream);
    }

    @Override
    public void encode(Integer value, OutputStream outStream) throws IOException {
      VarInt.encode(value, outStream);
    }
  }

  private static class CustomWindows extends NonMergingWindowFn<Integer, BoundedWindow> {
    @Override
    public Collection<BoundedWindow> assignWindows(final AssignContext c) throws Exception {
      return Collections.singleton(
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
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "%s is only compatible with %s.",
                CustomWindows.class.getSimpleName(), CustomWindows.class.getSimpleName()));
      }
    }

    @Override
    public Coder<BoundedWindow> windowCoder() {
      return new AtomicCoder<BoundedWindow>() {
        @Override
        public void verifyDeterministic() {}

        @Override
        public void encode(BoundedWindow value, OutputStream outStream) throws IOException {
          VarInt.encode(value.maxTimestamp().getMillis(), outStream);
        }

        @Override
        public BoundedWindow decode(InputStream inStream) throws IOException {
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
