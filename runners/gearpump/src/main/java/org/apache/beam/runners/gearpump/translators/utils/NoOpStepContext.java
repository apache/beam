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

package org.apache.beam.runners.gearpump.translators.utils;

import java.io.IOException;
import java.io.Serializable;

import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * serializable {@link ExecutionContext.StepContext} that basically does nothing.
 */
public class NoOpStepContext implements ExecutionContext.StepContext, Serializable {

  @Override
  public String getStepName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getTransformName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void noteOutput(WindowedValue<?> output) {
  }

  @Override
  public void noteSideOutput(TupleTag<?> tag, WindowedValue<?> output) {
  }

  @Override
  public <T, W extends BoundedWindow> void writePCollectionViewData(TupleTag<?> tag,
      Iterable<WindowedValue<T>> data,
      Coder<Iterable<WindowedValue<T>>> dataCoder, W window, Coder<W> windowCoder) throws
      IOException {
  }

  @Override
  public StateInternals<?> stateInternals() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TimerInternals timerInternals() {
    throw new UnsupportedOperationException();
  }
}
