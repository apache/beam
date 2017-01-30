/**
 * Copyright 2016 Seznam.cz, a.s.
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

import java.io.Serializable;

/**
 * A {@link Windowing} strategy associates each input element with a window
 * thereby grouping input elements into chunks
 * for further processing in small (micro-)batches.
 * <p>
 * Subclasses should implement {@code equals()} and {@code hashCode()} so that logically
 * same windows are treated the same.
 */
public abstract class Window implements Serializable {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);
}