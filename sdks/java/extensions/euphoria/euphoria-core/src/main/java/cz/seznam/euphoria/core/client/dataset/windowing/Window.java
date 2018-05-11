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
import java.io.Serializable;

/**
 * A {@link Windowing} strategy associates each input element with a window thereby grouping input
 * elements into chunks for further processing in small (micro-)batches.
 *
 * <p>Subclasses should implement {@code equals()}, {@code hashCode()} and {@code compareTo()} so
 * that logically same windows are treated the same.
 */
@Audience(Audience.Type.CLIENT)
public abstract class Window<T extends Window<T>> implements Serializable, Comparable<T> {

  /**
   * Return lowest timestamp greater than any element that can be present in this window.
   *
   * @return window fire and purge timestamp
   */
  public long maxTimestamp() {
    return Long.MAX_VALUE;
  }

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);
}
