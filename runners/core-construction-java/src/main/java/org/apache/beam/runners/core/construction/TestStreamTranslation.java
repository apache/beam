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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.runners.core.construction.PTransformTranslation.TEST_STREAM_TRANSFORM_URN;

import com.google.auto.service.AutoService;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Utility methods for translating a {@link TestStream} to and from {@link RunnerApi}
 * representations.
 */
public class TestStreamTranslation {

  static <T> RunnerApi.TestStreamPayload testStreamToPayload(
      TestStream<T> transform, SdkComponents components) throws IOException {
    String coderId = components.registerCoder(transform.getValueCoder());

    RunnerApi.TestStreamPayload.Builder builder =
        RunnerApi.TestStreamPayload.newBuilder().setCoderId(coderId);

    for (TestStream.Event<T> event : transform.getEvents()) {
      builder.addEvents(toProto(event, transform.getValueCoder()));
    }

    return builder.build();
  }

  private static TestStream<?> fromProto(
      RunnerApi.TestStreamPayload testStreamPayload, RehydratedComponents components)
      throws IOException {

    Coder<Object> coder = (Coder<Object>) components.getCoder(testStreamPayload.getCoderId());

    List<TestStream.Event<Object>> events = new ArrayList<>();

    for (RunnerApi.TestStreamPayload.Event event : testStreamPayload.getEventsList()) {
      events.add(fromProto(event, coder));
    }
    return TestStream.fromRawEvents(coder, events);
  }

  /**
   * Converts an {@link AppliedPTransform}, which may be a rehydrated transform or an original
   * {@link TestStream}, to a {@link TestStream}.
   */
  public static <T> TestStream<T> getTestStream(
      AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> application)
      throws IOException {
    // For robustness, we don't take this shortcut:
    // if (application.getTransform() instanceof TestStream) {
    //   return application.getTransform()
    // }

    SdkComponents sdkComponents = SdkComponents.create();
    RunnerApi.PTransform transformProto = PTransformTranslation.toProto(application, sdkComponents);
    checkArgument(
        TEST_STREAM_TRANSFORM_URN.equals(transformProto.getSpec().getUrn()),
        "Attempt to get %s from a transform with wrong URN %s",
        TestStream.class.getSimpleName(),
        transformProto.getSpec().getUrn());
    RunnerApi.TestStreamPayload testStreamPayload =
        RunnerApi.TestStreamPayload.parseFrom(transformProto.getSpec().getPayload());

    return (TestStream<T>)
        fromProto(
            testStreamPayload, RehydratedComponents.forComponents(sdkComponents.toComponents()));
  }

  static <T> RunnerApi.TestStreamPayload.Event toProto(TestStream.Event<T> event, Coder<T> coder)
      throws IOException {
    switch (event.getType()) {
      case WATERMARK:
        return RunnerApi.TestStreamPayload.Event.newBuilder()
            .setWatermarkEvent(
                RunnerApi.TestStreamPayload.Event.AdvanceWatermark.newBuilder()
                    .setNewWatermark(
                        ((TestStream.WatermarkEvent<T>) event).getWatermark().getMillis()))
            .build();

      case PROCESSING_TIME:
        return RunnerApi.TestStreamPayload.Event.newBuilder()
            .setProcessingTimeEvent(
                RunnerApi.TestStreamPayload.Event.AdvanceProcessingTime.newBuilder()
                    .setAdvanceDuration(
                        ((TestStream.ProcessingTimeEvent<T>) event)
                            .getProcessingTimeAdvance()
                            .getMillis()))
            .build();

      case ELEMENT:
        RunnerApi.TestStreamPayload.Event.AddElements.Builder builder =
            RunnerApi.TestStreamPayload.Event.AddElements.newBuilder();
        for (TimestampedValue<T> element : ((TestStream.ElementEvent<T>) event).getElements()) {
          builder.addElements(
              RunnerApi.TestStreamPayload.TimestampedElement.newBuilder()
                  .setTimestamp(element.getTimestamp().getMillis())
                  .setEncodedElement(
                      ByteString.copyFrom(
                          CoderUtils.encodeToByteArray(coder, element.getValue()))));
        }
        return RunnerApi.TestStreamPayload.Event.newBuilder().setElementEvent(builder).build();
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type of %s: %s",
                TestStream.Event.class.getCanonicalName(), event.getType()));
    }
  }

  static <T> TestStream.Event<T> fromProto(
      RunnerApi.TestStreamPayload.Event protoEvent, Coder<T> coder) throws IOException {
    switch (protoEvent.getEventCase()) {
      case WATERMARK_EVENT:
        return TestStream.WatermarkEvent.advanceTo(
            new Instant(protoEvent.getWatermarkEvent().getNewWatermark()));
      case PROCESSING_TIME_EVENT:
        return TestStream.ProcessingTimeEvent.advanceBy(
            Duration.millis(protoEvent.getProcessingTimeEvent().getAdvanceDuration()));
      case ELEMENT_EVENT:
        List<TimestampedValue<T>> decodedElements = new ArrayList<>();
        for (RunnerApi.TestStreamPayload.TimestampedElement element :
            protoEvent.getElementEvent().getElementsList()) {
          decodedElements.add(
              TimestampedValue.of(
                  CoderUtils.decodeFromByteArray(coder, element.getEncodedElement().toByteArray()),
                  new Instant(element.getTimestamp())));
        }
        return TestStream.ElementEvent.add(decodedElements);
      case EVENT_NOT_SET:
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported type of %s: %s",
                RunnerApi.TestStreamPayload.Event.class.getCanonicalName(),
                protoEvent.getEventCase()));
    }
  }

  static class TestStreamTranslator implements TransformPayloadTranslator<TestStream<?>> {
    @Override
    public String getUrn(TestStream<?> transform) {
      return TEST_STREAM_TRANSFORM_URN;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        AppliedPTransform<?, ?, TestStream<?>> transform, SdkComponents components)
        throws IOException {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(getUrn(transform.getTransform()))
          .setPayload(testStreamToPayload(transform.getTransform(), components).toByteString())
          .build();
    }
  }

  /** Registers {@link TestStreamTranslator}. */
  @AutoService(TransformPayloadTranslatorRegistrar.class)
  public static class Registrar implements TransformPayloadTranslatorRegistrar {
    @Override
    public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
        getTransformPayloadTranslators() {
      return Collections.singletonMap(TestStream.class, new TestStreamTranslator());
    }
  }
}
