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
package org.apache.beam.runners.core;

import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;

/**
 * An object that can output a value with all of its windowing information to the main output or any
 * tagged output.
 */
public interface OutputWindowedValue<OutputT> {
  /** Outputs a value with windowing information to the main output. */
  void outputWindowedValue(
      OutputT output,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo pane);

  /** Outputs a value with windowing information to a tagged output. */
  <AdditionalOutputT> void outputWindowedValue(
      TupleTag<AdditionalOutputT> tag,
      AdditionalOutputT output,
      Instant timestamp,
      Collection<? extends BoundedWindow> windows,
      PaneInfo pane);
}
