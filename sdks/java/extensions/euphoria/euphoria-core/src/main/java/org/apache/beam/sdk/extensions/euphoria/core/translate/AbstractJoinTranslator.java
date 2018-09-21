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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.type.TypeAwares;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

abstract class AbstractJoinTranslator<LeftT, RightT, KeyT, OutputT>
    implements OperatorTranslator<Object, KV<KeyT, OutputT>, Join<LeftT, RightT, KeyT, OutputT>> {

  @Override
  public PCollection<KV<KeyT, OutputT>> translate(
      Join<LeftT, RightT, KeyT, OutputT> operator, PCollectionList<Object> inputs) {
    checkArgument(inputs.size() == 2, "Join expects exactly two inputs.");
    @SuppressWarnings("unchecked")
    final PCollection<LeftT> left = (PCollection) inputs.get(0);
    @SuppressWarnings("unchecked")
    final PCollection<RightT> right = (PCollection) inputs.get(1);
    PCollection<KV<KeyT, LeftT>> leftKeyed =
        left.apply(
            "extract-keys-left",
            new ExtractKey<>(
                operator.getLeftKeyExtractor(), TypeAwares.orObjects(operator.getKeyType())));
    PCollection<KV<KeyT, RightT>> rightKeyed =
        right.apply(
            "extract-keys-right",
            new ExtractKey<>(
                operator.getRightKeyExtractor(), TypeAwares.orObjects(operator.getKeyType())));
    // apply windowing if specified
    if (operator.getWindow().isPresent()) {
      @SuppressWarnings("unchecked")
      final Window<KV<KeyT, LeftT>> leftWindow = (Window) operator.getWindow().get();
      leftKeyed = leftKeyed.apply("window-left", leftWindow);
      @SuppressWarnings("unchecked")
      final Window<KV<KeyT, RightT>> rightWindow = (Window) operator.getWindow().get();
      rightKeyed = rightKeyed.apply("window-right", rightWindow);
    }
    return translate(operator, leftKeyed, rightKeyed)
        .setTypeDescriptor(
            operator
                .getOutputType()
                .orElseThrow(
                    () -> new IllegalStateException("Unable to infer output type descriptor.")));
  }

  abstract PCollection<KV<KeyT, OutputT>> translate(
      Join<LeftT, RightT, KeyT, OutputT> operator,
      PCollection<KV<KeyT, LeftT>> left,
      PCollection<KV<KeyT, RightT>> right);
}
