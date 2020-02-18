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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.SplittableParDo.ProcessKeyedElements;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.ArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker.BaseArgumentProvider;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Instant;

/**
 * Utility transforms and overrides for running bounded splittable DoFn's naively, by implementing
 * {@link ProcessKeyedElements} using a simple {@link Reshuffle} and {@link ParDo}.
 */
public class SplittableParDoNaiveBounded {
  /** Overrides a {@link ProcessKeyedElements} into {@link SplittableProcessNaive}. */
  public static class OverrideFactory<InputT, OutputT, RestrictionT>
      implements PTransformOverrideFactory<
          PCollection<KV<byte[], KV<InputT, RestrictionT>>>,
          PCollectionTuple,
          ProcessKeyedElements<InputT, OutputT, RestrictionT>> {
    @Override
    public PTransformReplacement<
            PCollection<KV<byte[], KV<InputT, RestrictionT>>>, PCollectionTuple>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<byte[], KV<InputT, RestrictionT>>>,
                    PCollectionTuple,
                    ProcessKeyedElements<InputT, OutputT, RestrictionT>>
                transform) {
      checkArgument(
          DoFnSignatures.signatureForDoFn(transform.getTransform().getFn()).isBoundedPerElement()
              == IsBounded.BOUNDED,
          "Expecting a bounded-per-element splittable DoFn");
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new SplittableProcessNaive<>(transform.getTransform()));
    }

    @Override
    public Map<PValue, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PValue> outputs, PCollectionTuple newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }

  static class SplittableProcessNaive<
          InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT, ?>>
      extends PTransform<PCollection<KV<byte[], KV<InputT, RestrictionT>>>, PCollectionTuple> {
    private final ProcessKeyedElements<InputT, OutputT, RestrictionT> original;

    SplittableProcessNaive(ProcessKeyedElements<InputT, OutputT, RestrictionT> original) {
      this.original = original;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<byte[], KV<InputT, RestrictionT>>> input) {
      return input
          .apply("Drop key", Values.create())
          .apply("Reshuffle", Reshuffle.of())
          .apply(
              "NaiveProcess",
              ParDo.of(
                      new NaiveProcessFn<InputT, OutputT, RestrictionT, TrackerT>(original.getFn()))
                  .withSideInputs(original.getSideInputs())
                  .withOutputTags(original.getMainOutputTag(), original.getAdditionalOutputTags()));
    }
  }

  static class NaiveProcessFn<InputT, OutputT, RestrictionT, PositionT>
      extends DoFn<KV<InputT, RestrictionT>, OutputT> {
    private final DoFn<InputT, OutputT> fn;

    @Nullable private transient DoFnInvoker<InputT, OutputT> invoker;

    NaiveProcessFn(DoFn<InputT, OutputT> fn) {
      this.fn = fn;
    }

    @Setup
    public void setup() {
      this.invoker = DoFnInvokers.invokerFor(fn);
      invoker.invokeSetup();
    }

    @StartBundle
    public void startBundle(StartBundleContext c) {
      invoker.invokeStartBundle(
          new DoFn<InputT, OutputT>.StartBundleContext() {
            @Override
            public PipelineOptions getPipelineOptions() {
              return c.getPipelineOptions();
            }
          });
    }

    @ProcessElement
    public void process(ProcessContext c, BoundedWindow w) {
      RestrictionT restriction = c.element().getValue();
      while (true) {
        RestrictionT finalRestriction = restriction;
        RestrictionTracker<RestrictionT, PositionT> tracker =
            invoker.invokeNewTracker(
                new BaseArgumentProvider<InputT, OutputT>() {
                  @Override
                  public RestrictionT restriction() {
                    return finalRestriction;
                  }

                  @Override
                  public String getErrorContext() {
                    return NaiveProcessFn.class.getSimpleName() + ".invokeNewTracker";
                  }
                });
        ProcessContinuation continuation =
            invoker.invokeProcessElement(
                new NestedProcessContext<>(fn, c, c.element().getKey(), w, tracker));
        if (continuation.shouldResume()) {
          restriction = tracker.trySplit(0).getResidual();
          Uninterruptibles.sleepUninterruptibly(
              continuation.resumeDelay().getMillis(), TimeUnit.MILLISECONDS);
        } else {
          break;
        }
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      invoker.invokeFinishBundle(
          new DoFn<InputT, OutputT>.FinishBundleContext() {
            @Override
            public PipelineOptions getPipelineOptions() {
              return c.getPipelineOptions();
            }

            @Override
            public void output(@Nullable OutputT output, Instant timestamp, BoundedWindow window) {
              throw new UnsupportedOperationException(
                  "Output from FinishBundle for SDF is not supported");
            }

            @Override
            public <T> void output(
                TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
              throw new UnsupportedOperationException(
                  "Output from FinishBundle for SDF is not supported");
            }
          });
    }

    @Teardown
    public void teardown() {
      invoker.invokeTeardown();
    }

    private static class NestedProcessContext<
            InputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT, ?>>
        extends DoFn<InputT, OutputT>.ProcessContext implements ArgumentProvider<InputT, OutputT> {

      private final BoundedWindow window;
      private final DoFn<KV<InputT, RestrictionT>, OutputT>.ProcessContext outerContext;
      private final InputT element;
      private final TrackerT tracker;

      private NestedProcessContext(
          DoFn<InputT, OutputT> fn,
          DoFn<KV<InputT, RestrictionT>, OutputT>.ProcessContext outerContext,
          InputT element,
          BoundedWindow window,
          TrackerT tracker) {
        fn.super();
        this.window = window;
        this.outerContext = outerContext;
        this.element = element;
        this.tracker = tracker;
      }

      @Override
      public BoundedWindow window() {
        return window;
      }

      @Override
      public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
        return outerContext.pane();
      }

      @Override
      public PipelineOptions pipelineOptions() {
        return outerContext.getPipelineOptions();
      }

      @Override
      public DoFn<InputT, OutputT>.ProcessContext processContext(DoFn<InputT, OutputT> doFn) {
        return this;
      }

      @Override
      public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(DoFn<InputT, OutputT> doFn) {
        throw new IllegalStateException();
      }

      @Override
      public InputT element(DoFn<InputT, OutputT> doFn) {
        return element;
      }

      @Override
      public Object sideInput(String tagId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public TimerMap timerFamily(String tagId) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object schemaElement(int index) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Instant timestamp(DoFn<InputT, OutputT> doFn) {
        return outerContext.timestamp();
      }

      @Override
      public String timerId(DoFn<InputT, OutputT> doFn) {
        throw new UnsupportedOperationException();
      }

      @Override
      public OutputReceiver<OutputT> outputReceiver(DoFn<InputT, OutputT> doFn) {
        return new OutputReceiver<OutputT>() {
          @Override
          public void output(OutputT output) {
            outerContext.output(output);
          }

          @Override
          public void outputWithTimestamp(OutputT output, Instant timestamp) {
            outerContext.outputWithTimestamp(output, timestamp);
          }
        };
      }

      @Override
      public MultiOutputReceiver taggedOutputReceiver(DoFn<InputT, OutputT> doFn) {
        return new MultiOutputReceiver() {
          @Override
          public <T> OutputReceiver<T> get(TupleTag<T> tag) {
            return new OutputReceiver<T>() {
              @Override
              public void output(T output) {
                outerContext.output(tag, output);
              }

              @Override
              public void outputWithTimestamp(T output, Instant timestamp) {
                outerContext.outputWithTimestamp(tag, output, timestamp);
              }
            };
          }

          @Override
          public <T> OutputReceiver<Row> getRowReceiver(TupleTag<T> tag) {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public Object restriction() {
        return tracker.currentRestriction();
      }

      @Override
      public RestrictionTracker<?, ?> restrictionTracker() {
        return tracker;
      }

      @Override
      public PipelineOptions getPipelineOptions() {
        return outerContext.getPipelineOptions();
      }

      @Override
      public void output(OutputT output) {
        outerContext.output(output);
      }

      @Override
      public void outputWithTimestamp(OutputT output, Instant timestamp) {
        outerContext.outputWithTimestamp(output, timestamp);
      }

      @Override
      public <T> void output(TupleTag<T> tag, T output) {
        outerContext.output(tag, output);
      }

      @Override
      public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        outerContext.outputWithTimestamp(tag, output, timestamp);
      }

      @Override
      public InputT element() {
        return element;
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view) {
        return outerContext.sideInput(view);
      }

      @Override
      public Instant timestamp() {
        return outerContext.timestamp();
      }

      @Override
      public PaneInfo pane() {
        return outerContext.pane();
      }

      @Override
      public void updateWatermark(Instant watermark) {
        // Ignore watermark updates
      }

      // ----------- Unsupported methods --------------------

      @Override
      public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
          DoFn<InputT, OutputT> doFn) {
        throw new IllegalStateException();
      }

      @Override
      public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
          DoFn<InputT, OutputT> doFn) {
        throw new IllegalStateException();
      }

      @Override
      public OutputReceiver<Row> outputRowReceiver(DoFn<InputT, OutputT> doFn) {
        throw new UnsupportedOperationException();
      }

      @Override
      public TimeDomain timeDomain(DoFn<InputT, OutputT> doFn) {
        throw new IllegalStateException();
      }

      @Override
      public State state(String stateId, boolean alwaysFetched) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Timer timer(String timerId) {
        throw new UnsupportedOperationException();
      }
    }
  }
}
