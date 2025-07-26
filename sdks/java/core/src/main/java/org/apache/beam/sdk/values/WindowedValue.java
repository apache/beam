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

import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;

/**
 * A value along with Beam's windowing information and all other metadata.
 *
 * @param <T> the type of the primary data for the value.
 */
public interface WindowedValue<T> {
  /** The primary data for this value. */
  T getValue();

  /** The timestamp of this value in event time. */
  Instant getTimestamp();

  /** Returns the windows of this {@code WindowedValue}. */
  Collection<? extends BoundedWindow> getWindows();

  /** The {@link PaneInfo} associated with this WindowedValue. */
  PaneInfo getPaneInfo();

  /**
   * A representation of each of the actual values represented by this compressed {@link
   * WindowedValue}, one per window.
   */
  Iterable<WindowedValue<T>> explodeWindows();

  /**
   * A {@link WindowedValue} with identical metadata to the current one, but with the provided
   * value.
   */
  <OtherT> WindowedValue<OtherT> withValue(OtherT value);
}
