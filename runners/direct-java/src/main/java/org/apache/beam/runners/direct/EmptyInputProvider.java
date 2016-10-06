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
package org.apache.beam.runners.direct;

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * A {@link RootInputProvider} that provides a singleton empty bundle.
 */
class EmptyInputProvider implements RootInputProvider {
  private final EvaluationContext evaluationContext;

  EmptyInputProvider(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
  }

  /**
   * {@inheritDoc}.
   *
   * <p>Returns a single empty bundle. This bundle ensures that any {@link PTransform PTransforms}
   * that consume from the output of the provided {@link AppliedPTransform} have watermarks updated
   * as appropriate.
   */
  @Override
  public Collection<CommittedBundle<?>> getInitialInputs(AppliedPTransform<?, ?, ?> transform) {
    return Collections.<CommittedBundle<?>>singleton(
        evaluationContext.createRootBundle().commit(BoundedWindow.TIMESTAMP_MAX_VALUE));
  }
}
