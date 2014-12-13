/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.values;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.transforms.PTransform;

import java.util.Collection;

/**
 * The abstract interface of things that might be output from a
 * {@link PTransform}.
 */
public interface POutput {
  /**
   * Expands this {@code POutput} into a list of its component output
   * {@code PValue}s.
   *
   * <p> A {@link PValue} expands to itself.
   *
   * <p> A tuple or list of {@code PValue}s (e.g.,
   * {@link PCollectionTuple}, and
   * {@link PCollectionList}) expands to its component {@code PValue}s.
   *
   * <p> Not intended to be invoked directly by user code.
   */
  public Collection<? extends PValue> expand();

  /**
   * Records that this {@code POutput} is an output of the given
   * {@code PTransform} in the given {@code Pipeline}.
   *
   * <p> Should expand this {@code POutput} and invoke
   * {@link PValue#recordAsOutput(Pipeline,
   * com.google.cloud.dataflow.sdk.transforms.PTransform,
   * String)} on each component output {@code PValue}.
   *
   * <p> Automatically invoked as part of applying a
   * {@code PTransform}.  Not to be invoked directly by user code.
   */
  public void recordAsOutput(Pipeline pipeline,
                             PTransform<?, ?> transform);

  /**
   * As part of finishing the producing {@code PTransform}, finalizes this
   * {@code PTransform} output to make it ready for being used as an input and
   * for running.
   *
   * <p> This includes ensuring that all {@code PCollection}s
   * have {@code Coder}s specified or defaulted.
   *
   * <p> Automatically invoked whenever this {@code POutput} is used
   * as a {@code PInput} to another {@code PTransform}, or if never
   * used as a {@code PInput}, when {@link Pipeline#run} is called, so
   * users do not normally call this explicitly.
   */
  public void finishSpecifyingOutput();
}
