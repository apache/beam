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
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
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
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Utility transforms and overrides for running bounded splittable DoFn's naively, by implementing
 * {@link ProcessKeyedElements} using a simple {@link Reshuffle} and {@link ParDo}.
 */
public class SplittableParDoNaiveBounded {
  /** Overrides a {@link ProcessKeyedElements} into {@link SplittableProcessNaive}. */
  public static class OverrideFactory<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
      implements PTransformOverrideFactory<
          PCollection<KV<byte[], KV<InputT, RestrictionT>>>,
          PCollectionTuple,
          ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>> {
    @Override
    public PTransformReplacement<
            PCollection<KV<byte[], KV<InputT, RestrictionT>>>, PCollectionTuple>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<byte[], KV<InputT, RestrictionT>>>,
                    PCollectionTuple,
                    ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>>
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
          InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      extends PTransform<PCollection<KV<byte[], KV<InputT, RestrictionT>>>, PCollectionTuple> {
    private final ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT>
        original;

    SplittableProcessNaive(
        ProcessKeyedElements<InputT, OutputT, RestrictionT, WatermarkEstimatorStateT> original) {
      this.original = original;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<byte[], KV<InputT, RestrictionT>>> input) {
      return input
          .apply("Reshuffle", Reshuffle.of())
          .apply("Drop key", Values.create())
          .apply(
              "NaiveProcess",
              ParDo.of(
                      new NaiveProcessFn<
                          InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>(
                          original.getFn()))
                  .withSideInputs(original.getSideInputs())
                  .withOutputTags(original.getMainOutputTag(), original.getAdditionalOutputTags()));
    }
  }

  static class NaiveProcessFn<InputT, OutputT, RestrictionT, PositionT, WatermarkEstimatorStateT>
      extends DoFn<KV<InputT, RestrictionT>, OutputT> {
    private final DoFn<InputT, OutputT> fn;

    private transient @Nullable DoFnInvoker<InputT, OutputT> invoker;

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
          new BaseArgumentProvider<InputT, OutputT>() {
            @Override
            public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
                DoFn<InputT, OutputT> doFn) {
              return new DoFn<InputT, OutputT>.StartBundleContext() {
                @Override
                public PipelineOptions getPipelineOptions() {
                  return c.getPipelineOptions();
                }
              };
            }

            @Override
            public PipelineOptions pipelineOptions() {
              return c.getPipelineOptions();
            }

            @Override
            public String getErrorContext() {
              return "SplittableParDoNaiveBounded/StartBundle";
            }
          });
    }

    @ProcessElement
    public void process(ProcessContext c, BoundedWindow w) {
      WatermarkEstimatorStateT initialWatermarkEstimatorState =
          (WatermarkEstimatorStateT)
              invoker.invokeGetInitialWatermarkEstimatorState(
                  new BaseArgumentProvider<InputT, OutputT>() {
                    @Override
                    public InputT element(DoFn<InputT, OutputT> doFn) {
                      return c.element().getKey();
                    }

                    @Override
                    public Object restriction() {
                      return c.element().getValue();
                    }

                    @Override
                    public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                      return c.timestamp();
                    }

                    @Override
                    public PipelineOptions pipelineOptions() {
                      return c.getPipelineOptions();
                    }

                    @Override
                    public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
                      return c.pane();
                    }

                    @Override
                    public BoundedWindow window() {
                      return w;
                    }

                    @Override
                    public String getErrorContext() {
                      return NaiveProcessFn.class.getSimpleName()
                          + ".invokeGetInitialWatermarkEstimatorState";
                    }
                  });

      RestrictionT restriction = c.element().getValue();
      WatermarkEstimatorStateT watermarkEstimatorState = initialWatermarkEstimatorState;
      while (true) {
        RestrictionT currentRestriction = restriction;
        WatermarkEstimatorStateT currentWatermarkEstimatorState = watermarkEstimatorState;

        RestrictionTracker<RestrictionT, PositionT> tracker =
            invoker.invokeNewTracker(
                new BaseArgumentProvider<InputT, OutputT>() {
                  @Override
                  public InputT element(DoFn<InputT, OutputT> doFn) {
                    return c.element().getKey();
                  }

                  @Override
                  public RestrictionT restriction() {
                    return currentRestriction;
                  }

                  @Override
                  public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                    return c.timestamp();
                  }

                  @Override
                  public PipelineOptions pipelineOptions() {
                    return c.getPipelineOptions();
                  }

                  @Override
                  public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
                    return c.pane();
                  }

                  @Override
                  public BoundedWindow window() {
                    return w;
                  }

                  @Override
                  public String getErrorContext() {
                    return NaiveProcessFn.class.getSimpleName() + ".invokeNewTracker";
                  }
                });
        WatermarkEstimator<WatermarkEstimatorStateT> watermarkEstimator =
            invoker.invokeNewWatermarkEstimator(
                new BaseArgumentProvider<InputT, OutputT>() {
                  @Override
                  public InputT element(DoFn<InputT, OutputT> doFn) {
                    return c.element().getKey();
                  }

                  @Override
                  public RestrictionT restriction() {
                    return currentRestriction;
                  }

                  @Override
                  public WatermarkEstimatorStateT watermarkEstimatorState() {
                    return currentWatermarkEstimatorState;
                  }

                  @Override
                  public Instant timestamp(DoFn<InputT, OutputT> doFn) {
                    return c.timestamp();
                  }

                  @Override
                  public PipelineOptions pipelineOptions() {
                    return c.getPipelineOptions();
                  }

                  @Override
                  public PaneInfo paneInfo(DoFn<InputT, OutputT> doFn) {
                    return c.pane();
                  }

                  @Override
                  public BoundedWindow window() {
                    return w;
                  }

                  @Override
                  public String getErrorContext() {
                    return NaiveProcessFn.class.getSimpleName() + ".invokeNewWatermarkEstimator";
                  }
                });
        ProcessContinuation continuation =
            invoker.invokeProcessElement(
                new NestedProcessContext<>(
                    fn, c, c.element().getKey(), w, tracker, watermarkEstimator));
        if (continuation.shouldResume()) {
          // Fetch the watermark before splitting to ensure that the watermark applies to both
          // the primary and the residual.
          watermarkEstimatorState = watermarkEstimator.getState();
          SplitResult<RestrictionT> split = tracker.trySplit(0);
          if (split == null) {
            break;
          }
          restriction = split.getResidual();
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
          new BaseArgumentProvider<InputT, OutputT>() {
            @Override
            public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
                DoFn<InputT, OutputT> doFn) {
              return new DoFn<InputT, OutputT>.FinishBundleContext() {
                @Override
                public PipelineOptions getPipelineOptions() {
                  return c.getPipelineOptions();
                }

                @Override
                public void output(
                    @Nullable OutputT output, Instant timestamp, BoundedWindow window) {
                  throw new UnsupportedOperationException(
                      "Output from FinishBundle for SDF is not supported in naive implementation");
                }

                @Override
                public <T> void output(
                    TupleTag<T> tag, T output, Instant timestamp, BoundedWindow window) {
                  throw new UnsupportedOperationException(
                      "Output from FinishBundle for SDF is not supported in naive implementation");
                }
              };
            }

            @Override
            public PipelineOptions pipelineOptions() {
              return c.getPipelineOptions();
            }

            @Override
            public String getErrorContext() {
              return "SplittableParDoNaiveBounded/StartBundle";
            }
          });
    }

    @Teardown
    public void teardown() {
      invoker.invokeTeardown();
    }

    private static class NestedProcessContext<
            InputT,
            OutputT,
            RestrictionT,
            TrackerT extends RestrictionTracker<RestrictionT, ?>,
            WatermarkEstimatorStateT,
            WatermarkEstimatorT extends WatermarkEstimator<WatermarkEstimatorStateT>>
        extends DoFn<InputT, OutputT>.ProcessContext implements ArgumentProvider<InputT, OutputT> {

      private final BoundedWindow window;
      private final DoFn<KV<InputT, RestrictionT>, OutputT>.ProcessContext outerContext;
      private final InputT element;
      private final TrackerT tracker;
      private final WatermarkEstimatorT watermarkEstimator;

      private NestedProcessContext(
          DoFn<InputT, OutputT> fn,
          DoFn<KV<InputT, RestrictionT>, OutputT>.ProcessContext outerContext,
          InputT element,
          BoundedWindow window,
          TrackerT tracker,
          WatermarkEstimatorT watermarkEstimator) {
        fn.super();
        this.window = window;
        this.outerContext = outerContext;
        this.element = element;
        this.tracker = tracker;
        this.watermarkEstimator = watermarkEstimator;
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
      public Object key() {
        throw new UnsupportedOperationException();
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
      public BundleFinalizer bundleFinalizer() {
        throw new UnsupportedOperationException();
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
      public Object watermarkEstimatorState() {
        throw new UnsupportedOperationException(
            "@WatermarkEstimatorState parameters are not supported.");
      }

      @Override
      public WatermarkEstimator<?> watermarkEstimator() {
        return watermarkEstimator;
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
