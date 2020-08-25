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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.InMemoryBundleFinalizer;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.direct.WatermarkManager.TimerUpdate;
import org.apache.beam.runners.direct.WatermarkManager.TransformWatermarks;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execution Context for the {@link DirectRunner}.
 *
 * <p>This implementation is not thread safe. A new {@link DirectExecutionContext} must be created
 * for each thread that requires it.
 */
class DirectExecutionContext {
  private static final Logger LOG = LoggerFactory.getLogger(DirectExecutionContext.class);
  private final Clock clock;
  private final StructuralKey<?> key;
  private final CopyOnAccessInMemoryStateInternals existingState;
  private final TransformWatermarks watermarks;
  private Map<String, DirectStepContext> cachedStepContexts = new LinkedHashMap<>();

  public DirectExecutionContext(
      Clock clock,
      StructuralKey<?> key,
      CopyOnAccessInMemoryStateInternals existingState,
      TransformWatermarks watermarks) {
    this.clock = clock;
    this.key = key;
    this.existingState = existingState;
    this.watermarks = watermarks;
  }

  private DirectStepContext createStepContext() {
    return new DirectStepContext();
  }

  /** Returns the {@link StepContext} associated with the given step. */
  public DirectStepContext getStepContext(String stepName) {
    return cachedStepContexts.computeIfAbsent(stepName, k -> createStepContext());
  }

  /** Step Context for the {@link DirectRunner}. */
  public class DirectStepContext implements StepContext {
    private CopyOnAccessInMemoryStateInternals<?> stateInternals;
    private DirectTimerInternals timerInternals;
    private InMemoryBundleFinalizer bundleFinalizer;

    public DirectStepContext() {}

    @Override
    public CopyOnAccessInMemoryStateInternals<?> stateInternals() {
      if (stateInternals == null) {
        stateInternals = CopyOnAccessInMemoryStateInternals.withUnderlying(key, existingState);
      }
      return stateInternals;
    }

    @Override
    public DirectTimerInternals timerInternals() {
      if (timerInternals == null) {
        timerInternals = DirectTimerInternals.create(clock, watermarks, TimerUpdate.builder(key));
      }
      return timerInternals;
    }

    @Override
    public BundleFinalizer bundleFinalizer() {
      if (bundleFinalizer == null) {
        bundleFinalizer = new InMemoryBundleFinalizer();
      }
      return bundleFinalizer;
    }

    /** Returns any pending finalizations clearing internal state. */
    public List<InMemoryBundleFinalizer.Finalization> getAndClearFinalizations() {
      if (bundleFinalizer == null) {
        return Collections.EMPTY_LIST;
      }
      return bundleFinalizer.getAndClearFinalizations();
    }

    /**
     * Commits the state of this step, and returns the committed state. If the step has not accessed
     * any state, return null.
     */
    public CopyOnAccessInMemoryStateInternals commitState() {
      if (stateInternals != null) {
        return stateInternals.commit();
      }
      return null;
    }

    /**
     * Gets the timer update of the {@link TimerInternals} of this {@link DirectStepContext}, which
     * is empty if the {@link TimerInternals} were never accessed.
     */
    public TimerUpdate getTimerUpdate() {
      if (timerInternals == null) {
        return TimerUpdate.empty();
      }
      return timerInternals.getTimerUpdate();
    }
  }
}
