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
package cz.seznam.euphoria.core.client.dataset.windowing;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.triggers.NoopTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import java.io.ObjectStreamException;
import java.util.Collections;

/** Windowing with single window across the whole dataset. Suitable for batch processing. */
@Audience(Audience.Type.CLIENT)
public final class GlobalWindowing<T> implements Windowing<T, GlobalWindowing.Window> {

  private static final GlobalWindowing<?> INSTANCE = new GlobalWindowing<>();
  private static final Iterable<Window> INSTANCE_ITER = Collections.singleton(Window.INSTANCE);

  private GlobalWindowing() {}

  @SuppressWarnings("unchecked")
  public static <T> GlobalWindowing<T> get() {
    return (GlobalWindowing) INSTANCE;
  }

  @Override
  public Iterable<Window> assignWindowsToElement(WindowedElement<?, T> el) {
    return INSTANCE_ITER;
  }

  @Override
  public Trigger<Window> getTrigger() {
    return NoopTrigger.get();
  }

  private Object readResolve() throws ObjectStreamException {
    return INSTANCE;
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof GlobalWindowing;
  }

  @Override
  public int hashCode() {
    return 314159265;
  }

  /** Singleton window. */
  public static final class Window
      extends cz.seznam.euphoria.core.client.dataset.windowing.Window<GlobalWindowing.Window> {

    static final Window INSTANCE = new Window();

    private Window() {}

    public static Window get() {
      return INSTANCE;
    }

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
}
