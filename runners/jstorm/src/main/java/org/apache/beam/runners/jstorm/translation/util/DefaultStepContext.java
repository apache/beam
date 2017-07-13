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
package org.apache.beam.runners.jstorm.translation.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Default StepContext for running DoFn This does not allow accessing state or timer internals.
 */
public class DefaultStepContext implements ExecutionContext.StepContext {

  private TimerInternals timerInternals;

  private StateInternals stateInternals;

  public DefaultStepContext(TimerInternals timerInternals, StateInternals stateInternals) {
    this.timerInternals = checkNotNull(timerInternals, "timerInternals");
    this.stateInternals = checkNotNull(stateInternals, "stateInternals");
  }

  @Override
  public String getStepName() {
    return null;
  }

  @Override
  public String getTransformName() {
    return null;
  }

  @Override
  public void noteOutput(WindowedValue<?> windowedValue) {

  }

  @Override
  public void noteOutput(TupleTag<?> tupleTag, WindowedValue<?> windowedValue) {

  }

  @Override
  public <T, W extends BoundedWindow> void writePCollectionViewData(TupleTag<?> tag, Iterable<WindowedValue<T>> data,
                                                                    Coder<Iterable<WindowedValue<T>>> dataCoder, W window, Coder<W> windowCoder) throws IOException {
    throw new UnsupportedOperationException("Writing side-input data is not supported.");
  }

  @Override
  public StateInternals stateInternals() {
    return stateInternals;
  }

  @Override
  public TimerInternals timerInternals() {
    return timerInternals;
  }

  public void setStateInternals(StateInternals stateInternals) {
    this.stateInternals = stateInternals;
  }

  public void setTimerInternals(TimerInternals timerInternals) {
    this.timerInternals = timerInternals;
  }
}
