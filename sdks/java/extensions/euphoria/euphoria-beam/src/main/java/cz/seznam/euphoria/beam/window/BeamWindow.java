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
package cz.seznam.euphoria.beam.window;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * Wrapper around euphoria's {@code Window} into beam.
 */
public class BeamWindow<W extends Window<W>> extends BoundedWindow {

  private static final Instant MAX_TIMESTAMP = BoundedWindow.TIMESTAMP_MAX_VALUE.minus(1);
  private final W wrapped;

  private BeamWindow(W wrap) {
    this.wrapped = wrap;
  }

  @Override
  public Instant maxTimestamp() {
    // We cannot return more than MAX_TIMESTAMP-1 since beam's WatermarkManager checks every
    // window max timestamp to be smaller than BoundedWindow.TIMESTAMP_MAX_VALUE.

    long wrappedWindowMaxTimestamp = wrapped.maxTimestamp();
    if(wrappedWindowMaxTimestamp > MAX_TIMESTAMP.getMillis()){
      return MAX_TIMESTAMP;
    }

    return new Instant(wrappedWindowMaxTimestamp);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof BeamWindow && ((BeamWindow) obj).wrapped.equals(wrapped);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  public W get() {
    return wrapped;
  }

  public static <W extends Window<W>> BeamWindow<W> wrap(W wrap) {
    return new BeamWindow<>(wrap);
  }

}
