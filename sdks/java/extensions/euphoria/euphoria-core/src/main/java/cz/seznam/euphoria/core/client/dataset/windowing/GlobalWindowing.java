/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import java.io.ObjectStreamException;
import java.util.Collections;

/**
 * Windowing with single window across the whole dataset. Suitable for
 * batch processing.
 */
public final class GlobalWindowing<T>
    implements Windowing<T, GlobalWindowing.Window> {

  public static final class Window
      extends cz.seznam.euphoria.core.client.dataset.windowing.Window
      implements Comparable<Window> {

    static final Window INSTANCE = new Window();

    public static Window get() { return INSTANCE; }

    private Window() {}

    @Override
    public boolean equals(Object other) {
      return other instanceof GlobalWindowing.Window;
    }

    @Override
    public int hashCode() {
      return Integer.MAX_VALUE;
    }

    private Object readResolve() throws ObjectStreamException {
      return INSTANCE;
    }

    @Override
    public int compareTo(GlobalWindowing.Window o) {
      return 0;
    }
  } // ~ end of Label

  private final static GlobalWindowing<?> INSTANCE = new GlobalWindowing<>();
  private final static Iterable<Window> INSTANCE_ITER =
      Collections.singleton(Window.INSTANCE);

  private GlobalWindowing() {}

  @Override
  public Iterable<Window> assignWindowsToElement(WindowedElement<?, T> el) {
    return INSTANCE_ITER;
  }

  @Override
  public Trigger<Window> getTrigger() {
    return NoopTrigger.get();
  }

  @SuppressWarnings("unchecked")
  public static <T> GlobalWindowing<T> get() {
    return (GlobalWindowing) INSTANCE;
  }

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }
}
