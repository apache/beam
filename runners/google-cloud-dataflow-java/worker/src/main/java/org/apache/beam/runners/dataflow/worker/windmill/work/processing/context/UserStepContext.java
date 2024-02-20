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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.context;

import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * A specialized {@link DataflowStreamingModeStepContext} that uses provided {@link StateInternals}
 * and {@link TimerInternals} for user state and timers.
 */
class UserStepContext extends DataflowExecutionContext.DataflowStepContext
    implements StreamingModeStepContext {

  private final DataflowStreamingModeStepContext wrapped;

  UserStepContext(DataflowStreamingModeStepContext wrapped) {
    super(wrapped.getNameContext());
    this.wrapped = wrapped;
  }

  @Override
  public boolean issueSideInputFetch(PCollectionView<?> view, BoundedWindow w, SideInputState s) {
    return wrapped.issueSideInputFetch(view, w, s);
  }

  @Override
  public void addBlockingSideInput(Windmill.GlobalDataRequest blocked) {
    wrapped.addBlockingSideInput(blocked);
  }

  @Override
  public void addBlockingSideInputs(Iterable<Windmill.GlobalDataRequest> blocked) {
    wrapped.addBlockingSideInputs(blocked);
  }

  @Override
  public StateInternals stateInternals() {
    return wrapped.stateInternals();
  }

  @Override
  public Iterable<Windmill.GlobalDataId> getSideInputNotifications() {
    return wrapped.getSideInputNotifications();
  }

  @Override
  public <T, W extends BoundedWindow> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<T> data,
      Coder<Iterable<T>> dataCoder,
      W window,
      Coder<W> windowCoder) {
    throw new IllegalStateException("User DoFns cannot write PCollectionView data");
  }

  @Override
  public TimerInternals timerInternals() {
    return wrapped.userTimerInternals();
  }

  @Override
  public <W extends BoundedWindow> TimerInternals.TimerData getNextFiredTimer(
      Coder<W> windowCoder) {
    return wrapped.getNextFiredUserTimer(windowCoder);
  }

  @Override
  public <W extends BoundedWindow> void setStateCleanupTimer(
      String timerId,
      W window,
      Coder<W> windowCoder,
      Instant cleanupTime,
      Instant cleanupOutputTimestamp) {
    throw new UnsupportedOperationException(
        String.format(
            "setStateCleanupTimer should not be called on %s, only on a system %s",
            getClass().getSimpleName(), DataflowStreamingModeStepContext.class.getSimpleName()));
  }

  @Override
  public DataflowExecutionContext.DataflowStepContext namespacedToUser() {
    return this;
  }
}
