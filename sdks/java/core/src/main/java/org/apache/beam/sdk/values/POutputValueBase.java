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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * A {@link POutputValueBase} is the abstract base class of
 * {@code PTransform} outputs.
 *
 * <p>A {@link PValueBase} that adds tracking of its producing
 * {@link AppliedPTransform}.
 *
 * <p>For internal use.
 */
public abstract class POutputValueBase implements POutput {

  private final Pipeline pipeline;

  protected POutputValueBase(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  /**
   * No-arg constructor for Java serialization only.
   * The resulting {@link POutputValueBase} is unlikely to be
   * valid.
   */
  protected POutputValueBase() {
    pipeline = null;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  /**
   * Records that this {@link POutputValueBase} is an output with the given name of the given {@link
   * AppliedPTransform}.
   *
   * <p>By default, does nothing.
   *
   * <p>To be invoked only by {@link POutput#recordAsOutput} implementations. Not to be invoked
   * directly by user code.
   */
  @Override
  public void recordAsOutput(AppliedPTransform<?, ?, ?> transform) {}

  /**
   * Default behavior for {@link #finishSpecifyingOutput(PInput, PTransform)}} is
   * to do nothing. Override if your {@link PValue} requires
   * finalization.
   */
  @Override
  public void finishSpecifyingOutput(PInput input, PTransform<?, ?> transform) { }
}
