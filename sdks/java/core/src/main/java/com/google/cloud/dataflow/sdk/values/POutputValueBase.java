/*
 * Copyright (C) 2015 Google Inc.
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
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;

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
   * Returns the {@link AppliedPTransform} that this {@link POutputValueBase}
   * is an output of.
   *
   * <p>For internal use only.
   */
  public AppliedPTransform<?, ?, ?> getProducingTransformInternal() {
    return producingTransform;
  }

  /**
   * Records that this {@link POutputValueBase} is an output with the
   * given name of the given {@link AppliedPTransform}.
   *
   * <p>To be invoked only by {@link POutput#recordAsOutput}
   * implementations.  Not to be invoked directly by user code.
   */
  @Override
  public void recordAsOutput(AppliedPTransform<?, ?, ?> transform) {
    if (producingTransform != null) {
      // Already used this POutput as a PTransform output.  This can
      // happen if the POutput is an output of a transform within a
      // composite transform, and is also the result of the composite.
      // We want to record the "immediate" atomic transform producing
      // this output, and ignore all later composite transforms that
      // also produce this output.
      //
      // Pipeline.applyInternal() uses !hasProducingTransform() to
      // avoid calling this operation redundantly, but
      // hasProducingTransform() doesn't apply to POutputValueBases
      // that aren't PValues or composites of PValues, e.g., PDone.
      return;
    }
    producingTransform = transform;
  }

  /**
   * Default behavior for {@link #finishSpecifyingOutput()} is
   * to do nothing. Override if your {@link PValue} requires
   * finalization.
   */
  @Override
  public void finishSpecifyingOutput() { }

  /**
   * The {@link PTransform} that produces this {@link POutputValueBase}.
   */
  private AppliedPTransform<?, ?, ?> producingTransform;
}
