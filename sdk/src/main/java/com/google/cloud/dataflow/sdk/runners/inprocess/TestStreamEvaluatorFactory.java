/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.inprocess;

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.testing.TestStream;
import com.google.cloud.dataflow.sdk.testing.TestStream.ElementEvent;
import com.google.cloud.dataflow.sdk.testing.TestStream.Event;
import com.google.cloud.dataflow.sdk.testing.TestStream.EventType;
import com.google.cloud.dataflow.sdk.testing.TestStream.ProcessingTimeEvent;
import com.google.cloud.dataflow.sdk.testing.TestStream.WatermarkEvent;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.base.Supplier;
import org.joda.time.Duration;
import org.joda.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/** The {@link TransformEvaluatorFactory} for the {@link TestStream} primitive. */
class TestStreamEvaluatorFactory implements TransformEvaluatorFactory {
  private final KeyedResourcePool<AppliedPTransform<?, ?, ?>, Evaluator<?>> evaluators =
      LockedKeyedResourcePool.create();

  @Nullable
  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext)
      throws Exception {
    return createEvaluator((AppliedPTransform) application, evaluationContext);
  }

  /**
   * Returns the evaluator for the provided application of {@link TestStream}, or null if it is
   * already in use.
   *
   * <p>The documented behavior of {@link TestStream} requires the output of one event to travel
   * completely through the pipeline before any additional event, so additional instances that have
   * a separate collection of events cannot be created.
   */
  private <InputT, OutputT> TransformEvaluator<? super InputT> createEvaluator(
      AppliedPTransform<PBegin, PCollection<OutputT>, TestStream<OutputT>> application,
      InProcessEvaluationContext evaluationContext)
      throws ExecutionException {
    return evaluators
        .tryAcquire(application, new CreateEvaluator<>(application, evaluationContext, evaluators))
        .orNull();
  }

  private static class Evaluator<T> implements TransformEvaluator<Object> {
    private final AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application;
    private final InProcessEvaluationContext context;
    private final KeyedResourcePool<AppliedPTransform<?, ?, ?>, Evaluator<?>> cache;
    private final List<Event<T>> events;
    private int index;
    private Instant currentWatermark;

    private Evaluator(
        AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application,
        InProcessEvaluationContext context,
        KeyedResourcePool<AppliedPTransform<?, ?, ?>, Evaluator<?>> cache) {
      this.application = application;
      this.context = context;
      this.cache = cache;
      this.events = application.getTransform().getEvents();
      index = 0;
      currentWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public void processElement(WindowedValue<Object> element) throws Exception {}

    @Override
    public InProcessTransformResult finishBundle() throws Exception {
      try {
        if (index >= events.size()) {
          return StepTransformResult.withoutHold(application).build();
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
            bundle.add(
                WindowedValue.timestampedValueInGlobalWindow(elem.getValue(), elem.getTimestamp()));
          }
          result.addOutput(bundle);
        }
        if (event.getType().equals(EventType.PROCESSING_TIME)) {
          ((TestClock) context.getClock())
              .advance(((ProcessingTimeEvent<T>) event).getProcessingTimeAdvance());
        }
        index++;
        return result.build();
      } finally {
        cache.release(application, this);
      }
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

  static class InProcessTestStreamFactory implements PTransformOverrideFactory {
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
        PipelineRunner runner = input.getPipeline().getRunner();
        checkState(
            runner instanceof InProcessPipelineRunner,
            "%s can only be used when running with the %s",
            getClass().getSimpleName(),
            InProcessPipelineRunner.class.getSimpleName());
        input.getPipeline()
            .getOptions()
            .as(InProcessPipelineOptions.class)
            .setShutdownUnboundedProducersWithMaxWatermark(true);
        ((InProcessPipelineRunner) runner).setClockSupplier(new TestClockSupplier());
        return PCollection.<T>createPrimitiveOutputInternal(
            input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED)
            .setCoder(original.getValueCoder());
      }
    }
  }

  private static class CreateEvaluator<OutputT> implements Callable<Evaluator<?>> {
    private final AppliedPTransform<PBegin, PCollection<OutputT>, TestStream<OutputT>> application;
    private final InProcessEvaluationContext evaluationContext;
    private final KeyedResourcePool<AppliedPTransform<?, ?, ?>, Evaluator<?>> evaluators;

    public CreateEvaluator(
        AppliedPTransform<PBegin, PCollection<OutputT>, TestStream<OutputT>> application,
        InProcessEvaluationContext evaluationContext,
        KeyedResourcePool<AppliedPTransform<?, ?, ?>, Evaluator<?>> evaluators) {
      this.application = application;
      this.evaluationContext = evaluationContext;
      this.evaluators = evaluators;
    }

    @Override
    public Evaluator<?> call() throws Exception {
      return new Evaluator<>(application, evaluationContext, evaluators);
    }
  }
}

