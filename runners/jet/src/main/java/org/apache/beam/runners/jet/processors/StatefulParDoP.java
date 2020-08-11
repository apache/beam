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
package org.apache.beam.runners.jet.processors;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Jet {@link com.hazelcast.jet.core.Processor} implementation for Beam's stateful ParDo primitive.
 */
public class StatefulParDoP<OutputT>
    extends AbstractParDoP<KV<?, ?>, OutputT> { // todo: unify with ParDoP?

  private KeyedStepContext keyedStepContext;

  private StatefulParDoP(
      DoFn<KV<?, ?>, OutputT> doFn,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<TupleTag<?>, int[]> outputCollToOrdinals,
      SerializablePipelineOptions pipelineOptions,
      TupleTag<OutputT> mainOutputTag,
      Coder<KV<?, ?>> inputCoder,
      Map<PCollectionView<?>, Coder<?>> sideInputCoders,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      Coder<KV<?, ?>> inputValueCoder,
      Map<TupleTag<?>, Coder<?>> outputValueCoders,
      Map<Integer, PCollectionView<?>> ordinalToSideInput,
      Map<String, PCollectionView<?>> sideInputMapping,
      String ownerId,
      String stepId) {
    super(
        doFn,
        windowingStrategy,
        doFnSchemaInformation,
        outputCollToOrdinals,
        pipelineOptions,
        mainOutputTag,
        inputCoder,
        sideInputCoders,
        outputCoders,
        inputValueCoder,
        outputValueCoders,
        ordinalToSideInput,
        sideInputMapping,
        ownerId,
        stepId);
  }

  private static void fireTimer(
      Object key,
      TimerInternals.TimerData timer, DoFnRunner<KV<?, ?>, ?> doFnRunner) {
    StateNamespace namespace = timer.getNamespace();
    BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
    doFnRunner.onTimer(
        timer.getTimerId(),
        timer.getTimerFamilyId(),
        key,
        window,
        timer.getTimestamp(),
        timer.getOutputTimestamp(),
        timer.getDomain());
  }

  @Override
  protected DoFnRunner<KV<?, ?>, OutputT> getDoFnRunner(
      PipelineOptions pipelineOptions,
      DoFn<KV<?, ?>, OutputT> doFn,
      SideInputReader sideInputReader,
      JetOutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      Coder<KV<?, ?>> inputValueCoder,
      Map<TupleTag<?>, Coder<?>> outputValueCoders,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping) {
    keyedStepContext = new KeyedStepContext();
    return DoFnRunners.simpleRunner(
        pipelineOptions,
        doFn,
        sideInputReader,
        outputManager,
        mainOutputTag,
        additionalOutputTags,
        keyedStepContext,
        inputValueCoder,
        outputValueCoders,
        windowingStrategy,
        doFnSchemaInformation,
        sideInputMapping);
  }

  @Override
  protected void startRunnerBundle(DoFnRunner<KV<?, ?>, OutputT> runner) {
    keyedStepContext.advanceProcessingTimes();
    super.startRunnerBundle(runner);
  }

  @Override
  protected void processElementWithRunner(
      DoFnRunner<KV<?, ?>, OutputT> runner, WindowedValue<KV<?, ?>> windowedValue) {
    KV<?, ?> kv = windowedValue.getValue();
    Object key = kv.getKey();

    keyedStepContext.setKey(key);
    super.processElementWithRunner(runner, windowedValue);
    keyedStepContext.clearKey();
  }

  @Override
  public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
    return flushTimers(watermark.timestamp()) && super.tryProcessWatermark(watermark);
  }

  @Override
  public boolean complete() {
    return flushTimers(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) && super.complete();
  }

  private boolean flushTimers(long watermark) {
    keyedStepContext.flushTimers(watermark);
    return outputManager.tryFlush();
  }

  private void fireEligibleTimers(Object key, InMemoryTimerInternals timerInternals) {
    while (true) {
      TimerInternals.TimerData timer;
      boolean hasFired = false;

      while ((timer = timerInternals.removeNextEventTimer()) != null) {
        hasFired = true;
        fireTimer(key, timer, doFnRunner);
      }

      while ((timer = timerInternals.removeNextProcessingTimer()) != null) {
        hasFired = true;
        fireTimer(key, timer, doFnRunner);
      }

      while ((timer = timerInternals.removeNextSynchronizedProcessingTimer()) != null) {
        hasFired = true;
        fireTimer(key, timer, doFnRunner);
      }

      if (!hasFired) {
        break;
      }
    }
  }

  /**
   * Jet {@link Processor} supplier that will provide instances of {@link StatefulParDoP}.
   *
   * @param <OutputT> the type of main output elements of the DoFn being used
   */
  public static class Supplier<OutputT> extends AbstractSupplier<KV<?, ?>, OutputT> {

    public Supplier(
        String stepId,
        String ownerId,
        DoFn<KV<?, ?>, OutputT> doFn,
        WindowingStrategy<?, ?> windowingStrategy,
        DoFnSchemaInformation doFnSchemaInformation,
        SerializablePipelineOptions pipelineOptions,
        TupleTag<OutputT> mainOutputTag,
        Set<TupleTag<OutputT>> allOutputTags,
        Coder<KV<?, ?>> inputCoder,
        Map<PCollectionView<?>, Coder<?>> sideInputCoders,
        Map<TupleTag<?>, Coder<?>> outputCoders,
        Coder<KV<?, ?>> inputValueCoder,
        Map<TupleTag<?>, Coder<?>> outputValueCoders,
        Collection<PCollectionView<?>> sideInputs,
        Map<String, PCollectionView<?>> sideInputMapping) {
      super(
          stepId,
          ownerId,
          doFn,
          windowingStrategy,
          doFnSchemaInformation,
          pipelineOptions,
          mainOutputTag,
          allOutputTags,
          inputCoder,
          sideInputCoders,
          outputCoders,
          inputValueCoder,
          outputValueCoders,
          sideInputs,
          sideInputMapping);
    }

    @Override
    Processor getEx(
        DoFn<KV<?, ?>, OutputT> doFn,
        WindowingStrategy<?, ?> windowingStrategy,
        DoFnSchemaInformation doFnSchemaInformation,
        Map<TupleTag<?>, int[]> outputCollToOrdinals,
        SerializablePipelineOptions pipelineOptions,
        TupleTag<OutputT> mainOutputTag,
        Coder<KV<?, ?>> inputCoder,
        Map<PCollectionView<?>, Coder<?>> sideInputCoders,
        Map<TupleTag<?>, Coder<?>> outputCoders,
        Coder<KV<?, ?>> inputValueCoder,
        Map<TupleTag<?>, Coder<?>> outputValueCoders,
        Map<Integer, PCollectionView<?>> ordinalToSideInput,
        Map<String, PCollectionView<?>> sideInputMapping,
        String ownerId,
        String stepId) {
      return new StatefulParDoP<>(
          doFn,
          windowingStrategy,
          doFnSchemaInformation,
          outputCollToOrdinals,
          pipelineOptions,
          mainOutputTag,
          inputCoder,
          sideInputCoders,
          outputCoders,
          inputValueCoder,
          outputValueCoders,
          ordinalToSideInput,
          sideInputMapping,
          ownerId,
          stepId);
    }
  }

  private class KeyedStepContext implements StepContext {

    private final Object NULL = new Object();

    private final ConcurrentHashMap<Object, InMemoryStateInternals> keyedStateInternals;
    private final ConcurrentHashMap<Object, InMemoryTimerInternals> keyedTimerInternals;

    @SuppressWarnings("ThreadLocalUsage")
    private final ThreadLocal<Object> currentKey = new ThreadLocal<>();

    KeyedStepContext() {
      this.keyedStateInternals = new ConcurrentHashMap<>();
      this.keyedTimerInternals = new ConcurrentHashMap<>();
    }

    void setKey(Object key) {
      Object normalizedKey = key == null ? NULL : key;
      currentKey.set(normalizedKey);
      keyedStateInternals.computeIfAbsent(normalizedKey, InMemoryStateInternals::forKey);
      keyedTimerInternals.computeIfAbsent(normalizedKey, k -> new InMemoryTimerInternals());
    }

    void clearKey() {
      currentKey.remove();
    }

    @Override
    public StateInternals stateInternals() {
      Object key = currentKey.get();
      if (key == null) {
        throw new IllegalStateException("Active key should be set");
      }
      return keyedStateInternals.get(key);
    }

    @Override
    public TimerInternals timerInternals() {
      Object key = currentKey.get();
      if (key == null) {
        throw new IllegalStateException("Active key should be set");
      }
      return keyedTimerInternals.get(key);
    }

    public void advanceProcessingTimes() {
      Instant now = Instant.now();
      keyedTimerInternals.values()
          .forEach(timerInternals -> {
            try {
              timerInternals.advanceProcessingTime(now);
              timerInternals.advanceSynchronizedProcessingTime(now);
            } catch (Exception e) {
              throw new RuntimeException("Failed advancing time!");
            }
          });
    }

    public void flushTimers(long watermark) {
      Instant watermarkInstant = new Instant(watermark);
      keyedTimerInternals.entrySet().forEach(
          (entry) -> {
            InMemoryTimerInternals timerInternals = entry.getValue();
            if (timerInternals.currentInputWatermarkTime().isBefore(watermark)) {
              try {
                timerInternals.advanceInputWatermark(watermarkInstant);
                if (watermarkInstant.equals(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
                  timerInternals.advanceProcessingTime(watermarkInstant);
                  timerInternals.advanceSynchronizedProcessingTime(watermarkInstant);
                }
                Object key = entry.getKey();
                setKey(key);
                fireEligibleTimers(key, timerInternals);
                clearKey();
              } catch (Exception e) {
                throw new RuntimeException("Failed advancing processing time", e);
              }
            }
          }
      );
    }
  }

}
