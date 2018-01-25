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
package org.apache.beam.sdk.transforms;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;

/** A utility for testing properties of a splittable {@link DoFn}. */
public class SplittableDoFnTester<InputT, OutputT, RestrictionT> {
  private final PipelineOptions options;
  private final DoFn<InputT, OutputT> fn;
  private final DoFnInvoker<InputT, OutputT> invoker;
  private final ValueInSingleWindow<InputT> element;
  private boolean includeTimestampsAndWindows = false;

  /**
   * Initializes the tester for testing the given fn on the given element, with various
   * restrictions.
   */
  public SplittableDoFnTester(
      PipelineOptions options, DoFn<InputT, OutputT> fn, ValueInSingleWindow<InputT> element) {
    this.options = options;
    this.fn = fn;
    this.invoker = DoFnInvokers.invokerFor(fn);
    this.element = element;
  }

  /** Whether assertion error messages should include timestamps and windows. False by default. */
  public void setIncludeTimestampsAndWindows(boolean includeTimestampsAndWindows) {
    this.includeTimestampsAndWindows = includeTimestampsAndWindows;
  }

  private String elementString() {
    return includeTimestampsAndWindows ? element.toString() : String.valueOf(element.getValue());
  }

  /** Represents the outputs from the DoFn. */
  public class TaggedOutputs {
    private final Multiset<TimestampedValue<OutputT>> outputs;

    private TaggedOutputs(Multiset<TimestampedValue<OutputT>> outputs) {
      this.outputs = outputs;
    }

    /** The main output. */
    public Collection<TimestampedValue<OutputT>> getMain() {
      return outputs;
    }

    @Override
    public String toString() {
      return includeTimestampsAndWindows ? outputs.toString() : dropTimestamps(outputs).toString();
    }

    private TaggedOutputs plus(TaggedOutputs other) {
      return new TaggedOutputs(Multisets.sum(outputs, other.outputs));
    }

    private TaggedOutputs minus(TaggedOutputs other) {
      return new TaggedOutputs(Multisets.difference(outputs, other.outputs));
    }

    private boolean isEmpty() {
      return outputs.isEmpty();
    }

    private <T> Collection<T> dropTimestamps(Collection<TimestampedValue<T>> ts) {
      List<T> res = Lists.newArrayList();
      for (TimestampedValue<T> t : ts) {
        res.add(t.getValue());
      }
      return res;
    }
  }

  /** The result of processing one or more checkpoints. */
  public class PrefixResult {
    private final TaggedOutputs outputs;
    private final ProcessContinuation continuation;
    private final List<RestrictionT> completed;
    private final RestrictionT residual;

    private PrefixResult(
        TaggedOutputs outputs,
        ProcessContinuation continuation,
        List<RestrictionT> completed,
        RestrictionT residual) {
      this.outputs = outputs;
      this.continuation = continuation;
      this.completed = completed;
      this.residual = residual;
    }

    /** What was output as a result of this processing. */
    public TaggedOutputs getOutputs() {
      return outputs;
    }

    /** The last {@link ProcessContinuation}. */
    public ProcessContinuation getContinuation() {
      return continuation;
    }

    /** The completed restrictions. */
    public List<RestrictionT> getCompleted() {
      return completed;
    }

    /** The last residual restriction. */
    public RestrictionT getResidual() {
      return residual;
    }

    private PrefixResult unionWith(PrefixResult next) {
      return new PrefixResult(
          outputs.plus(next.outputs),
          next.continuation,
          Lists.newArrayList(Iterables.concat(completed, next.completed)),
          next.residual);
    }
  }

  public static final int NO_LIMIT = Integer.MAX_VALUE;

  /**
   * Invokes the {@link ProcessElement} call once on the given restriction, optionally requesting a
   * checkpoint after it emits the given number of outputs (unless the limit is {@link #NO_LIMIT}).
   */
  private PrefixResult processOnceWithoutVerifying(RestrictionT restriction, int numOutputsLimit) {
    final RestrictionTracker<RestrictionT> tracker = invoker.invokeNewTracker(restriction);
    final ProcessContext processContext = new ProcessContext(tracker, numOutputsLimit);

    ProcessContinuation continuation =
        invoker.invokeProcessElement(
            new DoFnInvoker.ArgumentProvider<InputT, OutputT>() {
              @Override
              public BoundedWindow window() {
                return element.getWindow();
              }

              @Override
              public PipelineOptions pipelineOptions() {
                return options;
              }

              @Override
              public RestrictionTracker<?> restrictionTracker() {
                return tracker;
              }

              @Override
              public DoFn<InputT, OutputT>.ProcessContext processContext(
                  DoFn<InputT, OutputT> doFn) {
                return processContext;
              }

              @Override
              public DoFn<InputT, OutputT>.StartBundleContext startBundleContext(
                  DoFn<InputT, OutputT> doFn) {
                throw new UnsupportedOperationException("StartBundle is not supported");
              }

              @Override
              public DoFn<InputT, OutputT>.FinishBundleContext finishBundleContext(
                  DoFn<InputT, OutputT> doFn) {
                throw new UnsupportedOperationException("FinishBundle is not supported");
              }

              @Override
              public DoFn<InputT, OutputT>.OnTimerContext onTimerContext(
                  DoFn<InputT, OutputT> doFn) {
                throw new UnsupportedOperationException("OnTimer is not supported");
              }

              @Override
              public State state(String stateId) {
                throw new UnsupportedOperationException("State is not supported");
              }

              @Override
              public Timer timer(String timerId) {
                throw new UnsupportedOperationException("Timers are not supported");
              }
            });

    RestrictionT residual = processContext.takenCheckpoint;
    if (residual == null && continuation.shouldResume()) {
      residual = tracker.checkpoint();
    }
    tracker.checkDone();
    RestrictionT primary = tracker.currentRestriction();
    return new PrefixResult(
        new TaggedOutputs(processContext.outputs), continuation, Arrays.asList(primary), residual);
  }

  /**
   * Invokes the {@link ProcessElement} call repeatedly on the given restriction and its residuals,
   * until the call returns {@link ProcessContinuation#stop}, and requesting a checkpoint when the
   * total number of outputs reaches the given limit.
   */
  private PrefixResult processRepeatedlyUntilLimitWithoutVerifying(
      RestrictionT restriction, int numOutputsLimit) {
    Multiset<TimestampedValue<OutputT>> outputs = LinkedHashMultiset.create();
    List<RestrictionT> completed = Lists.newArrayList();
    while (true) {
      PrefixResult res = processOnceWithoutVerifying(restriction, numOutputsLimit - outputs.size());
      outputs.addAll(res.getOutputs().getMain());
      completed.add(Iterables.getOnlyElement(res.getCompleted()));
      if (!res.getContinuation().shouldResume()) {
        return new PrefixResult(
            new TaggedOutputs(outputs), res.getContinuation(), completed, res.residual);
      }
      restriction = res.getResidual();
    }
  }

  /**
   * Invokes the {@link ProcessElement} call once on the given restriction, optionally requesting a
   * checkpoint after it emits the given number of outputs (unless the limit is {@link #NO_LIMIT}).
   *
   * <p>Also verifies that re-executing the primary restriction produces the same result as the
   * current call has produced.
   */
  public PrefixResult processOnce(RestrictionT restriction, int numOutputsLimit) {
    PrefixResult res = processOnceWithoutVerifying(restriction, numOutputsLimit);
    PrefixResult redonePrimary =
        processRepeatedlyUntilLimitWithoutVerifying(
            Iterables.getOnlyElement(res.getCompleted()), NO_LIMIT);
    assertEquals(
        format(
            "Processing element %s restriction %s with limit %s: primary restriction %s: "
                + "re-executing primary (actual) differs from "
                + "current ProcessElement output (expected)",
            elementString(), restriction, numOutputsLimit, res.getCompleted()),
        res.getOutputs().getMain(),
        redonePrimary.getOutputs().getMain());
    assertEquals(res.getOutputs().getMain(), redonePrimary.getOutputs().getMain());
    return res;
  }

  /**
   * Invokes the {@link ProcessElement} call repeatedly on the given restriction and its residuals,
   * until the call returns {@link ProcessContinuation#stop}, and requesting a checkpoint when the
   * total number of outputs reaches the given limit.
   *
   * <p>Also verifies that re-executing the completed restrictions produces the same result.
   */
  public PrefixResult processRepeatedlyUntilLimit(RestrictionT restriction, int numOutputsLimit) {
    PrefixResult res = processRepeatedlyUntilLimitWithoutVerifying(restriction, numOutputsLimit);
    Multiset<TimestampedValue<OutputT>> fromCompleted = LinkedHashMultiset.create();
    for (RestrictionT completed : res.getCompleted()) {
      PrefixResult completedRes = processRepeatedlyUntilLimitWithoutVerifying(completed, NO_LIMIT);
      fromCompleted.addAll(completedRes.getOutputs().getMain());
    }
    assertEquals(
        format(
            "Processing element %s restriction %s with limit %s: "
                + "re-executing all primary restrictions (Actual; %s) "
                + "differs from output so far (Expected)",
            elementString(), restriction, numOutputsLimit, res.getCompleted()),
        res.getOutputs().getMain(),
        fromCompleted);
    return res;
  }

  /**
   * Verifies that taking a checkpoint preserves data consistency.
   *
   * <pre>{@code
   * --------------------------------------------
   * |        |            | ...
   * --------------------------------------------
   *          inner        outer
   * }</pre>
   *
   * <p>Verifies that taking a checkpoint at "inner" does not alter the output as of "outer", where
   * both are specified by number of outputs. In particular, the following executions should
   * converge:
   *
   * <p><b>Expected:</b> take a checkpoint when number of outputs reaches "outer".
   *
   * <p><b>Actual:</b> take a checkpoint when number of outputs reaches "inner"; resume from the
   * residual; take another checkpoint when total number of outputs reaches "outer".
   *
   * <p>The exact outputs from these two executions may not be identical, but they must eventually
   * converge - in the sense that executing each of them some more (until "extra" more outputs are
   * produced) contains all outputs of the other (i.e. expected + extra >= actual; actual + extra >=
   * expected).
   */
  public void verifyCheckpointConsistency(
      RestrictionT restriction,
      int innerNumOutputsLimit,
      int outerNumOutputsLimit,
      int extraNumOutputsLimit) {
    PrefixResult expectedRes = processRepeatedlyUntilLimit(restriction, outerNumOutputsLimit);

    PrefixResult innerRes = processRepeatedlyUntilLimit(restriction, innerNumOutputsLimit);
    int innerThenOuterNumOutputsLimit =
        outerNumOutputsLimit - innerRes.getOutputs().getMain().size();
    PrefixResult innerThenOuterRes =
        processRepeatedlyUntilLimit(innerRes.getResidual(), innerThenOuterNumOutputsLimit);
    PrefixResult actualRes = innerRes.unionWith(innerThenOuterRes);

    PrefixResult expectedExtra =
        processRepeatedlyUntilLimitWithoutVerifying(
            expectedRes.getResidual(), extraNumOutputsLimit);
    TaggedOutputs expectedExtended = expectedRes.getOutputs().plus(expectedExtra.getOutputs());

    PrefixResult actualExtra =
        processRepeatedlyUntilLimitWithoutVerifying(actualRes.getResidual(), extraNumOutputsLimit);
    TaggedOutputs actualExtended = actualRes.getOutputs().plus(actualExtra.getOutputs());

    TaggedOutputs missingFromActual = actualRes.getOutputs().minus(expectedExtended);
    TaggedOutputs missingFromExpected = expectedRes.getOutputs().minus(actualExtended);

    String baseMessage =
        format("Processing element %s restriction %s: ", elementString(), restriction)
            + format(
                "Checkpointing after %d outputs causes divergence of first %d outputs.\n",
                innerNumOutputsLimit, outerNumOutputsLimit)
            + format(
                "Expected output (checkpointing after first %d outputs) produced by %s:\n%s\n",
                outerNumOutputsLimit, expectedRes.getCompleted(), expectedRes.getOutputs())
            + format(
                "Actual output (checkpointing after first %d outputs and then after another %d) ",
                innerNumOutputsLimit, innerThenOuterNumOutputsLimit)
            + format(
                "produced by %s (%d outputs) and %s (%d outputs):\n%s.",
                innerRes.getCompleted(),
                innerRes.getOutputs().getMain().size(),
                innerThenOuterRes.getCompleted(),
                innerThenOuterRes.getOutputs().getMain().size(),
                actualRes.getOutputs())
            + "\n";
    assertTrue(
        format(
            baseMessage + "Elements that did not appear in expected after %d extra outputs: %s",
            extraNumOutputsLimit,
            missingFromExpected),
        missingFromExpected.isEmpty());
    assertTrue(
        format(
            baseMessage + "Elements that did not appear in actual after %d extra outputs: %s",
            extraNumOutputsLimit,
            missingFromActual),
        missingFromActual.isEmpty());
  }

  private class ProcessContext extends DoFn<InputT, OutputT>.ProcessContext {
    private final RestrictionTracker<RestrictionT> tracker;
    private final int numOutputsLimit;

    private Multiset<TimestampedValue<OutputT>> outputs = LinkedHashMultiset.create();
    private RestrictionT takenCheckpoint;

    private ProcessContext(RestrictionTracker<RestrictionT> tracker, int numOutputsLimit) {
      fn.super();
      this.tracker = tracker;
      this.numOutputsLimit = numOutputsLimit;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return options;
    }

    @Override
    public InputT element() {
      return element.getValue();
    }

    @Override
    public Instant timestamp() {
      return element.getTimestamp();
    }

    @Override
    public PaneInfo pane() {
      return element.getPane();
    }

    @Override
    public void output(OutputT output) {
      outputWithTimestamp(output, timestamp());
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
      outputs.add(TimestampedValue.of(output, timestamp));
      if (outputs.size() >= numOutputsLimit && takenCheckpoint == null) {
        takenCheckpoint = tracker.checkpoint();
      }
    }

    @Override
    public <T> void output(TupleTag<T> tag, T output) {
      // Ignore
    }

    @Override
    public <T> void outputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      // Ignore
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      throw new UnsupportedOperationException("Side inputs are not supported");
    }

    @Override
    public void updateWatermark(Instant watermark) {
      // Ignore
    }
  }
}
