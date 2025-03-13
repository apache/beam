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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.sdk.util.construction.PTransformTranslation.TEST_STREAM_TRANSFORM_URN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.TestStreamPayload;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link TestStreamTranslation}. */
@RunWith(Parameterized.class)
public class TestStreamTranslationTest {
  @Parameters(name = "{index}: {0}")
  public static Iterable<TestStream<?>> data() {
    return ImmutableList.of(
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
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.TestStreamPayload payload =
        TestStreamTranslation.payloadForTestStream(testStream, components);

    verifyTestStreamEncoding(
        testStream, payload, RehydratedComponents.forComponents(components.toComponents()));
  }

  @Test
  public void testRegistrarEncodedProto() throws Exception {
    PCollection<String> output = p.apply(testStream);

    AppliedPTransform<PBegin, PCollection<String>, TestStream<String>> appliedTestStream =
        AppliedPTransform.of(
            "fakeName",
            PValues.expandInput(PBegin.in(p)),
            PValues.expandOutput(output),
            testStream,
            ResourceHints.create(),
            p);

    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.FunctionSpec spec =
        PTransformTranslation.toProto(appliedTestStream, components).getSpec();

    assertThat(spec.getUrn(), equalTo(TEST_STREAM_TRANSFORM_URN));

    RunnerApi.TestStreamPayload payload = TestStreamPayload.parseFrom(spec.getPayload());

    verifyTestStreamEncoding(
        testStream, payload, RehydratedComponents.forComponents(components.toComponents()));
  }

  private static <T> void verifyTestStreamEncoding(
      TestStream<T> testStream,
      RunnerApi.TestStreamPayload payload,
      RehydratedComponents protoComponents)
      throws Exception {

    // This reverse direction is only valid for Java-based coders
    assertThat(protoComponents.getCoder(payload.getCoderId()), equalTo(testStream.getValueCoder()));

    assertThat(payload.getEventsList().size(), equalTo(testStream.getEvents().size()));

    for (int i = 0; i < payload.getEventsList().size(); ++i) {
      assertThat(
          TestStreamTranslation.eventFromProto(payload.getEvents(i), testStream.getValueCoder()),
          equalTo(testStream.getEvents().get(i)));
    }
  }
}
