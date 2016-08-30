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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.runners.direct.DirectRunner.UncommittedBundle;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.TestStream.ElementEvent;
import org.apache.beam.sdk.testing.TestStream.Event;
import org.apache.beam.sdk.testing.TestStream.EventType;
import org.apache.beam.sdk.testing.TestStream.ProcessingTimeEvent;
import org.apache.beam.sdk.testing.TestStream.WatermarkEvent;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * The {@link TransformEvaluatorFactory} for the {@link TestStream} primitive.
 */
class TestStreamEvaluatorFactory implements TransformEvaluatorFactory {
  /**
   * A map from {@link TestStream} application to an {@link Optional} {@link Evaluator} for that
   * application. At most one evaluator is created for an AppliedPTransform and it is used by at
   * most one thread at a time.
   *
   * <p>For each {@link AppliedPTransform} in this map:
   * <ul>
   * <li>If there is no associated value, then no evaluator has been created yet.
   * <li>If the value is {@code Optional.absent()} then the evaluator is currently in use.
   * <li>If the value is {@code Optional.present()} then the contained evaluator is available for
   *     use.
   * </ul>
   */
  private final ConcurrentMap<AppliedPTransform<?, ?, ?>, Optional<Evaluator<?>>> evaluators =
      new ConcurrentHashMap<>();

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      EvaluationContext evaluationContext) throws Exception {
    return createEvaluator((AppliedPTransform) application, evaluationContext);
  }

  @Override
  public void cleanup() throws Exception {}

  /**
   * Returns the evaluator for the provided application of {@link TestStream}, or null if it is
   * already in use.
   *
   * <p>The documented behavior of {@link TestStream} requires the output of one event to travel
   * completely through the pipeline before any additional event, so additional instances that
   * have a separate collection of events cannot be created.
   */
  private <InputT, OutputT> TransformEvaluator<? super InputT> createEvaluator(
      AppliedPTransform<PBegin, PCollection<OutputT>, TestStream<OutputT>> application,
      EvaluationContext evaluationContext) {
    // Replace any existing value with absent, and get the existing value (atomically); ensures
    // only one thread can obtain the evaluator per-transform.
    Optional<Evaluator<?>> evaluator =
        evaluators.replace(application, Optional.<Evaluator<?>>absent());
    if (evaluator != null) {
      return evaluator.orNull();
    }
    Evaluator<OutputT> createdEvaluator =
        new Evaluator<>(application, evaluationContext, evaluators);
    evaluators.putIfAbsent(application, Optional.<Evaluator<?>>of(createdEvaluator));
    return evaluators.replace(application, Optional.<Evaluator<?>>absent()).orNull();
  }

  private static class Evaluator<T> implements TransformEvaluator<Object> {
    private final AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application;
    private final EvaluationContext context;
    private final ConcurrentMap<AppliedPTransform<?, ?, ?>, Optional<Evaluator<?>>> evaluators;
    private final List<Event<T>> events;
    private int index;
    private Instant currentWatermark;

    private Evaluator(
        AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application,
        EvaluationContext context,
        ConcurrentMap<AppliedPTransform<?, ?, ?>, Optional<Evaluator<?>>> evaluators) {
      this.application = application;
      this.context = context;
      this.events = application.getTransform().getEvents();
      this.evaluators = evaluators;
      index = 0;
      currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public void processElement(WindowedValue<Object> element) throws Exception {
    }

    @Override
    public TransformResult finishBundle() throws Exception {
      if (index >= events.size()) {
        return StepTransformResult.withHold(application, BoundedWindow.TIMESTAMP_MAX_VALUE).build();
      }
      Event<T> event = events.get(index);
      if (event.getType().equals(EventType.WATERMARK)) {
        currentWatermark = ((WatermarkEvent<T>) event).getWatermark();
      }
      StepTransformResult.Builder result =
          StepTransformResult.withHold(application, currentWatermark);
      if (event.getType().equals(EventType.ELEMENT)) {
        UncommittedBundle<T> bundle = context.createRootBundle(application.getOutput());
        for (TimestampedValue<T> elem : ((ElementEvent<T>) event).getElements()) {
          bundle.add(WindowedValue.timestampedValueInGlobalWindow(elem.getValue(),
              elem.getTimestamp()));
        }
        result.addOutput(bundle);
      }
      if (event.getType().equals(EventType.PROCESSING_TIME)) {
        ((TestClock) context.getClock())
            .advance(((ProcessingTimeEvent<T>) event).getProcessingTimeAdvance());
      }
      index++;
      checkState(
          !evaluators.replace(application, Optional.<Evaluator<?>>of(this)).isPresent(),
          "The evaluator for a %s was changed while the source evaluator was executing. "
              + "%s cannot be split or evaluated in parallel.",
          TestStream.class.getSimpleName(),
          TestStream.class.getSimpleName());
      return result.build();
    }
  }

  private static class TestClock implements Clock {
    private final AtomicReference<Instant> currentTime =
        new AtomicReference<>(BoundedWindow.TIMESTAMP_MIN_VALUE);

    public void advance(Duration amount) {
      Instant now = currentTime.get();
      currentTime.compareAndSet(now, now.plus(amount));
    }

    @Override
    public Instant now() {
      return currentTime.get();
    }
  }

  private static class TestClockSupplier implements Supplier<Clock> {
    @Override
    public Clock get() {
      return new TestClock();
    }
  }

  static class DirectTestStreamFactory implements PTransformOverrideFactory {
    @Override
    public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
        PTransform<InputT, OutputT> transform) {
      if (transform instanceof TestStream) {
        return (PTransform<InputT, OutputT>)
            new DirectTestStream<OutputT>((TestStream<OutputT>) transform);
      }
      return transform;
    }

    private static class DirectTestStream<T> extends PTransform<PBegin, PCollection<T>> {
      private final TestStream<T> original;

      private DirectTestStream(TestStream transform) {
        this.original = transform;
      }

      @Override
      public PCollection<T> apply(PBegin input) {
        setup(input.getPipeline());
        return PCollection.<T>createPrimitiveOutputInternal(
                input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
            .setCoder(original.getValueCoder());
      }

      private void setup(Pipeline p) {
        PipelineRunner runner = p.getRunner();
        checkState(runner instanceof DirectRunner,
            "%s can only be used when running with the %s",
            getClass().getSimpleName(),
            DirectRunner.class.getSimpleName());
        ((DirectRunner) runner).setClockSupplier(new TestClockSupplier());
      }
    }
  }
}
