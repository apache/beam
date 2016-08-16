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

import com.google.common.base.Supplier;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

/**
 * The {@link TransformEvaluatorFactory} for the {@link TestStream} primitive.
 */
class TestStreamEvaluatorFactory implements TransformEvaluatorFactory {
  private final AtomicBoolean inUse = new AtomicBoolean(false);
  private final AtomicReference<Evaluator<?>> evaluator = new AtomicReference<>();

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

  private <InputT, OutputT> TransformEvaluator<? super InputT> createEvaluator(
      AppliedPTransform<PBegin, PCollection<OutputT>, TestStream<OutputT>> application,
      EvaluationContext evaluationContext) {
    if (evaluator.get() == null) {
      Evaluator<OutputT> createdEvaluator = new Evaluator<>(application, evaluationContext, inUse);
      evaluator.compareAndSet(null, createdEvaluator);
    }
    if (inUse.compareAndSet(false, true)) {
      return evaluator.get();
    } else {
      return null;
    }
  }

  private static class Evaluator<T> implements TransformEvaluator<Object> {
    private final AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application;
    private final EvaluationContext context;
    private final AtomicBoolean inUse;
    private final List<Event<T>> events;
    private int index;
    private Instant currentWatermark;

    private Evaluator(
        AppliedPTransform<PBegin, PCollection<T>, TestStream<T>> application,
        EvaluationContext context,
        AtomicBoolean inUse) {
      this.application = application;
      this.context = context;
      this.inUse = inUse;
      this.events = application.getTransform().getEvents();
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
      checkState(inUse.compareAndSet(true, false),
          "The InUse flag of a %s was changed while the source evaluator was executing. "
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
