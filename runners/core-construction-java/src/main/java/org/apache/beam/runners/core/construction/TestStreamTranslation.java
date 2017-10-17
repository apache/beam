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
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation.TransformPayloadTranslator;
import org.apache.beam.sdk.coders.Coder;
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

  private interface TestStreamLike {
    Coder<?> getValueCoder();

    List<RunnerApi.TestStreamPayload.Event> getEvents();
  }

  @VisibleForTesting
  static class RawTestStream<T> extends PTransformTranslation.RawPTransform<PBegin, PCollection<T>>
      implements TestStreamLike {

    private final transient RehydratedComponents rehydratedComponents;
    private final RunnerApi.TestStreamPayload payload;
    private final Coder<T> valueCoder;
    private final RunnerApi.FunctionSpec spec;

    public RawTestStream(
        RunnerApi.TestStreamPayload payload, RehydratedComponents rehydratedComponents) {
      this.payload = payload;
      this.spec =
          RunnerApi.FunctionSpec.newBuilder()
              .setUrn(TEST_STREAM_TRANSFORM_URN)
              .setPayload(payload.toByteString())
              .build();
      this.rehydratedComponents = rehydratedComponents;

      // Eagerly extract the coder to throw a good exception here
      try {
        this.valueCoder = (Coder<T>) rehydratedComponents.getCoder(payload.getCoderId());
      } catch (IOException exc) {
        throw new IllegalArgumentException(
            String.format(
                "Failure extracting coder with id '%s' for %s",
                payload.getCoderId(), TestStream.class.getSimpleName()),
            exc);
      }
    }

    @Override
    public String getUrn() {
      return TEST_STREAM_TRANSFORM_URN;
    }

    @Nonnull
    @Override
    public RunnerApi.FunctionSpec getSpec() {
      return spec;
    }

    @Override
    public RunnerApi.FunctionSpec migrate(SdkComponents components) throws IOException {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(TEST_STREAM_TRANSFORM_URN)
          .setPayload(payloadForTestStreamLike(this, components).toByteString())
          .build();
    }

    @Override
    public Coder<T> getValueCoder() {
      return valueCoder;
    }

    @Override
    public List<RunnerApi.TestStreamPayload.Event> getEvents() {
      return payload.getEventsList();
    }
  }

  private static TestStream<?> testStreamFromProtoPayload(
      RunnerApi.TestStreamPayload testStreamPayload, RehydratedComponents components)
      throws IOException {

    Coder<Object> coder = (Coder<Object>) components.getCoder(testStreamPayload.getCoderId());

    List<TestStream.Event<Object>> events = new ArrayList<>();

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
  static class TestStreamTranslator implements TransformPayloadTranslator<TestStream<?>> {
    @Override
    public String getUrn(TestStream<?> transform) {
      return TEST_STREAM_TRANSFORM_URN;
    }

    @Override
    public RunnerApi.FunctionSpec translate(
        final AppliedPTransform<?, ?, TestStream<?>> transform, SdkComponents components)
        throws IOException {
      return translateTyped(transform.getTransform(), components);
    }

    @Override
    public PTransformTranslation.RawPTransform<?, ?> rehydrate(
        RunnerApi.PTransform protoTransform, RehydratedComponents rehydratedComponents)
        throws IOException {
      checkArgument(
          protoTransform.getSpec() != null,
          "%s received transform with null spec",
          getClass().getSimpleName());
      checkArgument(protoTransform.getSpec().getUrn().equals(TEST_STREAM_TRANSFORM_URN));
      return new RawTestStream<>(
          RunnerApi.TestStreamPayload.parseFrom(protoTransform.getSpec().getPayload()),
          rehydratedComponents);
    }

    private <T> RunnerApi.FunctionSpec translateTyped(
        final TestStream<T> testStream, SdkComponents components) throws IOException {
      return RunnerApi.FunctionSpec.newBuilder()
          .setUrn(TEST_STREAM_TRANSFORM_URN)
          .setPayload(payloadForTestStream(testStream, components).toByteString())
          .build();
    }

    /** Registers {@link TestStreamTranslator}. */
    @AutoService(TransformPayloadTranslatorRegistrar.class)
    public static class Registrar implements TransformPayloadTranslatorRegistrar {
      @Override
      public Map<? extends Class<? extends PTransform>, ? extends TransformPayloadTranslator>
          getTransformPayloadTranslators() {
        return Collections.singletonMap(TestStream.class, new TestStreamTranslator());
      }

      @Override
      public Map<String, ? extends TransformPayloadTranslator> getTransformRehydrators() {
        return Collections.singletonMap(TEST_STREAM_TRANSFORM_URN, new TestStreamTranslator());
      }
    }
  }

  /** Produces a {@link RunnerApi.TestStreamPayload} from a portable {@link RawTestStream}. */
  static RunnerApi.TestStreamPayload payloadForTestStreamLike(
      TestStreamLike transform, SdkComponents components) throws IOException {
    return RunnerApi.TestStreamPayload.newBuilder()
        .setCoderId(components.registerCoder(transform.getValueCoder()))
        .addAllEvents(transform.getEvents())
        .build();
  }

  @VisibleForTesting
  static <T> RunnerApi.TestStreamPayload payloadForTestStream(
      final TestStream<T> testStream, SdkComponents components) throws IOException {
    return payloadForTestStreamLike(
        new TestStreamLike() {
          @Override
          public Coder<T> getValueCoder() {
            return testStream.getValueCoder();
          }

          @Override
          public List<RunnerApi.TestStreamPayload.Event> getEvents() {
            try {
              List<RunnerApi.TestStreamPayload.Event> protoEvents = new ArrayList<>();
              for (TestStream.Event<T> event : testStream.getEvents()) {
                protoEvents.add(eventToProto(event, testStream.getValueCoder()));
              }
              return protoEvents;
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        },
        components);
  }
}
