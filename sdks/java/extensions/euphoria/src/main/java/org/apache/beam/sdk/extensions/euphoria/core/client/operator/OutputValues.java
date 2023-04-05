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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.translate.OperatorTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Internal operator used to extract values from keyed output.
 *
 * @param <InputT> input type
 * @param <KeyT> key type
 * @param <OutputT> output type
 */
class OutputValues<InputT, KeyT, OutputT> extends Operator<OutputT>
    implements CompositeOperator<InputT, OutputT> {

  private final Operator<KV<KeyT, OutputT>> keyedOperator;

  OutputValues(
      @Nullable String name,
      @Nullable TypeDescriptor<OutputT> outputType,
      Operator<KV<KeyT, OutputT>> keyedOperator) {
    super(name, outputType);
    this.keyedOperator = keyedOperator;
  }

  @Override
  public PCollection<OutputT> expand(PCollectionList<InputT> inputs) {
    final PCollection<KV<KeyT, OutputT>> keyedOutput =
        OperatorTransform.apply(keyedOperator, inputs);
    return MapElements.named("ExtractValues")
        .of(keyedOutput)
        .using(KV::getValue, getOutputType().orElse(null))
        .output();
  }
}
