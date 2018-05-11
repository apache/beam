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

/**
 * A single data element flowing in dataset. Every such element is associated with a window
 * identifier and timestamp.
 *
 * @param <W> type of the window
 * @param <T> type of the data element
 */
@Audience(Audience.Type.CLIENT)
public interface WindowedElement<W extends Window, T> {

  /** @return window of element. */
  W getWindow();

  /** @return associated timestamp */
  long getTimestamp();

  /** @return the data element itself */
  T getElement();
}
