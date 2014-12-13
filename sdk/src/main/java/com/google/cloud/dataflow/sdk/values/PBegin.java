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
import java.util.Collections;

/**
 * {@code PBegin} is used as the "input" to a root
 * {@link com.google.cloud.dataflow.sdk.transforms.PTransform} which
 * is the first operation in a {@link Pipeline}, such as
 * {@link com.google.cloud.dataflow.sdk.io.TextIO.Read} or
 * {@link com.google.cloud.dataflow.sdk.transforms.Create}.
 *
 * <p> Typically created by calling {@link Pipeline#begin} on a Pipeline.
 */
public class PBegin implements PInput {
  /**
   * Returns a {@code PBegin} in the given {@code Pipeline}.
   */
  public static PBegin in(Pipeline pipeline) {
    return new PBegin(pipeline);
  }

  /**
   * Applies the given PTransform to this input PBegin, and
   * returns the PTransform's Output.
   */
  public <Output extends POutput> Output apply(
      PTransform<? super PBegin, Output> t) {
    return Pipeline.applyTransform(this, t);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Collection<? extends PValue> expand() {
    // A PBegin contains no PValues.
    return Collections.emptyList();
  }

  @Override
  public void finishSpecifying() {
    // Nothing more to be done.
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Constructs a {@code PBegin} in the given {@code Pipeline}.
   */
  protected PBegin(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  private Pipeline pipeline;
}
