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
package cz.seznam.euphoria.beam.window;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import java.util.Objects;

/**
 * A {@code WindowedElement} created from Beam's element.
 */
public class BeamWindowedElement<W extends Window, T> implements WindowedElement<W, T> {

  private final T elem;
  private final W window;
  private final long stamp;

  private BeamWindowedElement(T elem, W window, long stamp) {
    this.elem = Objects.requireNonNull(elem);
    this.window = Objects.requireNonNull(window);
    this.stamp = stamp;
  }

  public static <W extends Window, T> BeamWindowedElement<W, T> of(T elem, W window, long stamp) {
    return new BeamWindowedElement<>(elem, window, stamp);
  }

  @Override
  public W getWindow() {
    return window;
  }

  @Override
  public long getTimestamp() {
    return stamp;
  }

  @Override
  public T getElement() {
    return elem;
  }
}
