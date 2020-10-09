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
package org.apache.beam.sdk.values;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;

/** Implementation of ValueInSingleWindow to support failsafe items in value. */
public final class FailsafeValueInSingleWindow<T, ErrorT>
    extends ValueInSingleWindow<T> {
  private final T value;
  private final Instant timestamp;
  private final BoundedWindow window;
  private final PaneInfo pane;
  private final ErrorT failsafeValue;

  // public <T, ErrorT> FailsafeValueInSingleWindow<T, ErrorT> of(
  //     T value, Instant timestamp, BoundedWindow window, PaneInfo paneInfo, ErrorT failsafeValue) {
  public FailsafeValueInSingleWindow(
      T value, Instant timestamp, BoundedWindow window, PaneInfo paneInfo, ErrorT failsafeValue) {
    this.value = value;
    this.failsafeValue = failsafeValue;
    if (timestamp == null) {
      throw new NullPointerException("Null timestamp");
    }
    this.timestamp = timestamp;
    if (window == null) {
      throw new NullPointerException("Null window");
    }
    this.window = window;
    if (paneInfo == null) {
      throw new NullPointerException("Null pane");
    }
    this.pane = paneInfo;
  }

  @Override
  public T getValue() {
    return value;
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public BoundedWindow getWindow() {
    return window;
  }

  @Override
  public PaneInfo getPane() {
    return pane;
  }

  public ErrorT getFailsafeValue() {
    return failsafeValue;
  }
}

