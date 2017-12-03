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
import cz.seznam.euphoria.core.client.triggers.CountTrigger;
import cz.seznam.euphoria.core.client.triggers.Trigger;

import static java.util.Collections.singleton;

/**
 * Count tumbling windowing.
 */
@Audience(Audience.Type.CLIENT)
public final class Count<T> implements Windowing<T, GlobalWindowing.Window> {

  private final int maxCount;

  private Count(int maxCount) {
    this.maxCount = maxCount;
  }

  @Override
  public Iterable<GlobalWindowing.Window> assignWindowsToElement(WindowedElement<?, T> el) {
    return singleton(GlobalWindowing.Window.get());
  }

  @Override
  public Trigger<GlobalWindowing.Window> getTrigger() {
    return new CountTrigger<>(maxCount);
  }

  public static <T> Count<T> of(int count) {
    return new Count<>(count);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Count) {
      return ((Count) obj).maxCount == maxCount;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return maxCount;
  }

}
