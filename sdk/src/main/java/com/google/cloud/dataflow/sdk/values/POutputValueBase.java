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

/**
 * A {@code POutputValueBase} is the abstract base class of
 * {@code PTransform} outputs.
 *
 * <p> A {@code PValueBase} that adds tracking of its producing
 * {@code PTransform}.
 *
 * <p> For internal use.
 */
public abstract class POutputValueBase implements POutput {

  protected POutputValueBase() { }

  /**
   * Returns the {@code PTransform} that this {@code POutputValueBase}
   * is an output of.
   *
   * <p> For internal use only.
   */
  public PTransform<?, ?> getProducingTransformInternal() {
    return producingTransform;
  }

  /**
   * Records that this {@code POutputValueBase} is an output with the
   * given name of the given {@code PTransform} in the given
   * {@code Pipeline}.
   *
   * <p> To be invoked only by {@link POutput#recordAsOutput}
   * implementations.  Not to be invoked directly by user code.
   */
  public void recordAsOutput(Pipeline pipeline,
                             PTransform<?, ?> transform) {
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
   * Default behavior for {@code finishSpecifyingOutput()} is
   * to do nothing. Override if your {@link PValue} requires
   * finalization.
   */
  public void finishSpecifyingOutput() { }

  /**
   * The {@code PTransform} that produces this {@code POutputValueBase}.
   */
  private PTransform<?, ?> producingTransform;
}
