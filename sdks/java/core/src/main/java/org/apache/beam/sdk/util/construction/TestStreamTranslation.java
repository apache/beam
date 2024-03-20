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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Utility methods for translating a {@link TestStream} to and from {@link RunnerApi}
 * representations.
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class TestStreamTranslation {

  public static <T> TestStream<T> testStreamFromProtoPayload(
      RunnerApi.TestStreamPayload testStreamPayload, RehydratedComponents components)
      throws IOException {

    Coder<T> coder = (Coder<T>) components.getCoder(testStreamPayload.getCoderId());

    return testStreamFromProtoPayload(testStreamPayload, coder);
  }

  public static <T> TestStream<T> testStreamFromProtoPayload(
      RunnerApi.TestStreamPayload testStreamPayload, Coder<T> coder) throws IOException {
    List<TestStream.Event<T>> events = new ArrayList<>();

    for (RunnerApi.TestStreamPayload.Event event : testStreamPayload.getEventsList()) {
      events.add(eventFromProto(event, coder));
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

    SdkComponents sdkComponents = SdkComponents.create(application.getPipeline().getOptions());
    RunnerApi.PTransform transformProto = PTransformTranslation.toProto(application, sdkComponents);
    Preconditions.checkArgument(
        PTransformTranslation.TEST_STREAM_TRANSFORM_URN.equals(transformProto.getSpec().getUrn()),
        "Attempt to get %s from a transform with wrong URN %s",
        TestStream.class.getSimpleName(),
        transformProto.getSpec().getUrn());
    RunnerApi.TestStreamPayload testStreamPayload =
        RunnerApi.TestStreamPayload.parseFrom(transformProto.getSpec().getPayload());

    return (TestStream<T>)
        testStreamFromProtoPayload(
            testStreamPayload, RehydratedComponents.forComponents(sdkComponents.toComponents()));
  }

  static <T> RunnerApi.TestStreamPayload.Event eventToProto(
      TestStream.Event<T> event, Coder<T> coder) throws IOException {
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

  static <T> TestStream.Event<T> eventFromProto(
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

  /** A translator registered to translate {@link TestStream} objects to protobuf representation. */
  static class TestStreamTranslator
      implements PTransformTranslation.TransformPayloadTranslator<TestStream<?>> {
    @Override
    public String getUrn() {
      return PTransformTranslation.TEST_STREAM_TRANSFORM_URN;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        final AppliedPTransform<?, ?, TestStream<?>> transform, SdkComponents components)
        throws IOException {
      return translateTyped(transform.getTransform(), components);
    }

    private <T> RunnerApi.FunctionSpec translateTyped(
        final TestStream<T> testStream, SdkComponents components) throws IOException {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(PTransformTranslation.TEST_STREAM_TRANSFORM_URN)
          .setPayload(payloadForTestStream(testStream, components).toByteString())
          .build();
    }

    /** Registers {@link TestStreamTranslator}. */
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Registrar implements TransformPayloadTranslatorRegistrar {
      @Override
      public Map<
              ? extends Class<? extends PTransform>,
              ? extends PTransformTranslation.TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(TestStream.class, new TestStreamTranslator());
      }
    }
  }

  /** Produces a {@link RunnerApi.TestStreamPayload} from a {@link TestStream}. */
  static <T> RunnerApi.TestStreamPayload payloadForTestStream(
      final TestStream<T> transform, SdkComponents components) throws IOException {
    List<RunnerApi.TestStreamPayload.Event> protoEvents = new ArrayList<>();
    try {
      for (TestStream.Event<T> event : transform.getEvents()) {
        protoEvents.add(eventToProto(event, transform.getValueCoder()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return RunnerApi.TestStreamPayload.newBuilder()
        .setCoderId(components.registerCoder(transform.getValueCoder()))
        .addAllEvents(protoEvents)
        .build();
  }
}
