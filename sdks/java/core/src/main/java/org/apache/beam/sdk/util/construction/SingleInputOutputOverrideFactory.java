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
package org.apache.beam.sdk.util.construction;

import java.util.Map;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link PTransformOverrideFactory} which consumes from a {@link PValue} and produces a {@link
 * PValue}. {@link #mapOutputs(Map, PValue)} is implemented.
 */
public abstract class SingleInputOutputOverrideFactory<
        InputT extends PValue,
        OutputT extends PValue,
        TransformT extends PTransform<InputT, OutputT>>
    implements PTransformOverrideFactory<InputT, OutputT, TransformT> {
  @Override
  public final Map<PCollection<?>, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PCollection<?>> outputs, OutputT newOutput) {
    return ReplacementOutputs.singleton(outputs, newOutput);
  }
}
