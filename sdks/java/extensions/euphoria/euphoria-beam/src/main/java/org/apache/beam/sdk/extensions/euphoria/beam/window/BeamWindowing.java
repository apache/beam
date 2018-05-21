/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.euphoria.beam.window;


import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.WindowedElement;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;

/**
 * Euphoria's {@link Windowing} which wraps Beam's {@link WindowFn}, {@link Trigger} and {@link
 * WindowingStrategy.AccumulationMode} to allow Beam's widowing to be defined through Euphoria API.
 */
public class BeamWindowing<T, BeamWinT extends BoundedWindow> implements
    Windowing<T, UnsupportedWindow> {

  private final WindowFn<?, BeamWinT> windowFn;
  private final Trigger trigger;
  private final WindowingStrategy.AccumulationMode accumulationMode;

  private BeamWindowing(WindowFn<?, BeamWinT> windowFn, Trigger beamTrigger,
      AccumulationMode accumulationMode) {
    this.windowFn = windowFn;
    this.trigger = beamTrigger;
    this.accumulationMode = accumulationMode;
  }

  public static <T, BeamWinT extends BoundedWindow> BeamWindowing<T, BeamWinT> of(

      WindowFn<?, BeamWinT> windowFn, Trigger trigger,
      WindowingStrategy.AccumulationMode accumulationMode) {
    return new BeamWindowing<>(windowFn, trigger, accumulationMode);
  }


  @Override
  public Iterable<UnsupportedWindow> assignWindowsToElement(WindowedElement<?, T> el) {
    throw new UnsupportedOperationException(
        "Beam window serves as envelope, it do not supports element to window assignment.");
  }

  @Override
  public org.apache.beam.sdk.extensions.euphoria.core.client.triggers.Trigger<UnsupportedWindow>
  getTrigger() {
    throw new UnsupportedOperationException(
        "Beam window serves as envelope, it do not contains Euphoria trigger.");
  }

  public WindowFn<?, BeamWinT> getWindowFn() {
    return windowFn;
  }

  public AccumulationMode getAccumulationMode() {
    return accumulationMode;
  }

  public Trigger getBeamTrigger() {
    return trigger;
  }
}
