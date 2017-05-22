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
package org.apache.beam.runners.core;

import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Base class for implementations of {@link StepContext}.
 *
 * <p>To complete a concrete subclass, implement {@link #timerInternals} and
 * {@link #stateInternals}.
 */
public abstract class BaseStepContext implements StepContext {
  private final String stepName;
  private final String transformName;

  public BaseStepContext(String stepName, String transformName) {
    this.stepName = stepName;
    this.transformName = transformName;
  }

  @Override
  public String getStepName() {
    return stepName;
  }

  @Override
  public String getTransformName() {
    return transformName;
  }

  @Override
  public <T, W extends BoundedWindow> void writePCollectionViewData(
      TupleTag<?> tag,
      Iterable<WindowedValue<T>> data, Coder<Iterable<WindowedValue<T>>> dataCoder,
      W window, Coder<W> windowCoder) throws IOException {
    throw new UnsupportedOperationException("Not implemented.");
  }

  @Override
  public abstract StateInternals stateInternals();

  @Override
  public abstract TimerInternals timerInternals();
}
