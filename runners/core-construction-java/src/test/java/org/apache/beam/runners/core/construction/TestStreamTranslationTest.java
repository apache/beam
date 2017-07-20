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

import static org.apache.beam.runners.core.construction.PTransformTranslation.TEST_STREAM_TRANSFORM_URN;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.beam.runners.core.construction.TestStreamTranslationTest.TestStreamPayloadTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;

/** Tests for {@link TestStreamTranslation}. */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  TestStreamPayloadTranslation.class,
})
public class TestStreamTranslationTest {

  /** Tests for translating various {@link ParDo} transforms to/from {@link ParDoPayload} protos. */
  @RunWith(Parameterized.class)
  public static class TestStreamPayloadTranslation {
    @Parameters(name = "{index}: {0}")
    public static Iterable<TestStream<?>> data() {
      return ImmutableList.<TestStream<?>>of(
          TestStream.create(VarIntCoder.of()).advanceWatermarkToInfinity(),
          TestStream.create(VarIntCoder.of())
              .advanceWatermarkTo(new Instant(42))
              .advanceWatermarkToInfinity(),
          TestStream.create(VarIntCoder.of())
              .addElements(TimestampedValue.of(3, new Instant(17)))
              .advanceWatermarkToInfinity(),
          TestStream.create(StringUtf8Coder.of())
              .advanceProcessingTime(Duration.millis(82))
              .advanceWatermarkToInfinity());
    }

    @Parameter(0)
    public TestStream<String> testStream;

    public static TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testEncodedProto() throws Exception {
      SdkComponents components = SdkComponents.create();
      RunnerApi.TestStreamPayload payload =
          TestStreamTranslation.testStreamToPayload(testStream, components);

      verifyTestStreamEncoding(
          testStream, payload, RehydratedComponents.forComponents(components.toComponents()));
    }

    @Test
    public void testRegistrarEncodedProto() throws Exception {
      PCollection<String> output = p.apply(testStream);

      AppliedPTransform<PBegin, PCollection<String>, TestStream<String>> appliedTestStream =
          AppliedPTransform.<PBegin, PCollection<String>, TestStream<String>>of(
              "fakeName", PBegin.in(p).expand(), output.expand(), testStream, p);

      SdkComponents components = SdkComponents.create();
      RunnerApi.FunctionSpec spec =
          PTransformTranslation.toProto(appliedTestStream, components).getSpec();

      assertThat(spec.getUrn(), equalTo(TEST_STREAM_TRANSFORM_URN));

      RunnerApi.TestStreamPayload payload =
          spec.getParameter().unpack(RunnerApi.TestStreamPayload.class);

      verifyTestStreamEncoding(
          testStream, payload, RehydratedComponents.forComponents(components.toComponents()));
    }

    private static <T> void verifyTestStreamEncoding(
        TestStream<T> testStream,
        RunnerApi.TestStreamPayload payload,
        RehydratedComponents protoComponents)
        throws Exception {

      // This reverse direction is only valid for Java-based coders
      assertThat(
          protoComponents.getCoder(payload.getCoderId()),
          Matchers.<Coder<?>>equalTo(testStream.getValueCoder()));

      assertThat(payload.getEventsList().size(), equalTo(testStream.getEvents().size()));

      for (int i = 0; i < payload.getEventsList().size(); ++i) {
        assertThat(
            TestStreamTranslation.fromProto(payload.getEvents(i), testStream.getValueCoder()),
            equalTo(testStream.getEvents().get(i)));
      }
    }
  }
}
