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

import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * The interface for values that can be input to and output from {@link PTransform PTransforms}.
 */
public interface PValue extends POutput, PInput {

  /**
   * Returns the name of this {@link PValue}.
   */
  String getName();

  /**
   * Returns the {@link AppliedPTransform} that this {@link PValue} is an output of.
   *
   * <p>For internal use only.
   */
  AppliedPTransform<?, ?, ?> getProducingTransformInternal();

  /**
   * Records that this {@code PValue} is an output of the given
   * {@code PTransform}, so long as this {@code PValue} is currently recorded as an output of
   * the original {@code PTransform}.
   *
   * <p>For a compound {@code PValue}, it is advised to call
   * this method on each component {@code PValue}.
   *
   * <p>This is not intended to be invoked by user code, but
   * is automatically invoked as part of replacing the
   * producing {@link PTransform}.
   */
  void replaceAsOutput(AppliedPTransform<?, ?, ?> original, AppliedPTransform<?, ?, ?> newProducer);
}
