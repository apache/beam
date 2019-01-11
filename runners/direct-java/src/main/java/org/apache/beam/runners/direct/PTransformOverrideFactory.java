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

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

interface PTransformOverrideFactory<
    InputT extends PInput,
    OutputT extends POutput,
    TransformT extends PTransform<InputT, OutputT>> {
  /**
   * Create a {@link PTransform} override for the provided {@link PTransform} if applicable.
   * Otherwise, return the input {@link PTransform}.
   *
   * <p>The returned PTransform must be semantically equivalent to the input {@link PTransform}.
   */
  PTransform<InputT, OutputT> override(TransformT transform);
}
