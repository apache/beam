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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.translate.common.OperatorTranslatorUtil;
import org.apache.beam.sdk.extensions.euphoria.core.translate.join.FullJoinFn;
import org.apache.beam.sdk.extensions.euphoria.core.translate.join.InnerJoinFn;
import org.apache.beam.sdk.extensions.euphoria.core.translate.join.JoinFn;
import org.apache.beam.sdk.extensions.euphoria.core.translate.join.LeftOuterJoinFn;
import org.apache.beam.sdk.extensions.euphoria.core.translate.join.RightOuterJoinFn;
import org.apache.beam.sdk.extensions.euphoria.core.translate.window.WindowingUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/** {@link OperatorTranslator Translator } for Euphoria {@link Join} operator. */
public class JoinTranslator implements OperatorTranslator<Join> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(Join operator, TranslationContext context) {
    return doTranslate(operator, context);
  }

  public <K, LeftT, RightT, OutputT, W extends BoundedWindow>
      PCollection<KV<K, OutputT>> doTranslate(
          Join<LeftT, RightT, K, OutputT, W> operator, TranslationContext context) {

    Coder<K> keyCoder = context.getKeyCoder(operator);

    // get input data-sets transformed to Pcollections<KV<K,LeftT/RightT>>
    @SuppressWarnings("unchecked")
    final PCollection<LeftT> left = (PCollection<LeftT>) context.getInputs(operator).get(0);
    Coder<LeftT> leftCoder = context.getCoderBasedOnDatasetElementType(operator.getLeft());

    @SuppressWarnings("unchecked")
    final PCollection<RightT> right = (PCollection<RightT>) context.getInputs(operator).get(1);
    Coder<RightT> rightCoder = context.getCoderBasedOnDatasetElementType(operator.getRight());

    PCollection<KV<K, LeftT>> leftKvInput =
        OperatorTranslatorUtil.getKVInputCollection(
            left, operator.getLeftKeyExtractor(), keyCoder, leftCoder, "::extract-keys-left");

    PCollection<KV<K, RightT>> rightKvInput =
        OperatorTranslatorUtil.getKVInputCollection(
            right, operator.getRightKeyExtractor(), keyCoder, rightCoder, "::extract-keys-right");

    // and apply the same widowing on input PColections since the documentation states:
    //'all of the PCollections you want to group must use the same
    // windowing strategy and window sizing'
    leftKvInput =
        WindowingUtils.applyWindowingIfSpecified(
            operator, leftKvInput, context.getAllowedLateness(operator));
    rightKvInput =
        WindowingUtils.applyWindowingIfSpecified(
            operator, rightKvInput, context.getAllowedLateness(operator));

    // GoGroupByKey collections
    TupleTag<LeftT> leftTag = new TupleTag<>();
    TupleTag<RightT> rightTag = new TupleTag<>();

    WindowingUtils.checkGropupByKeyApplicalble(operator, leftKvInput, rightKvInput);

    PCollection<KV<K, CoGbkResult>> coGrouped =
        KeyedPCollectionTuple.of(leftTag, leftKvInput)
            .and(rightTag, rightKvInput)
            .apply("::co-group-by-key", CoGroupByKey.create());

    final AccumulatorProvider accumulators =
        new LazyAccumulatorProvider(context.getAccumulatorFactory(), context.getSettings());

    // Join
    JoinFn<LeftT, RightT, K, OutputT> joinFn =
        chooseJoinFn(operator, leftTag, rightTag, accumulators);

    PCollection<KV<K, OutputT>> output = coGrouped.apply(joinFn.getFnName(), ParDo.of(joinFn));
    output.setCoder(context.getOutputCoder(operator));

    return output;
  }

  private <K, LeftT, RightT, OutputT, W extends BoundedWindow>
      JoinFn<LeftT, RightT, K, OutputT> chooseJoinFn(
          Join<LeftT, RightT, K, OutputT, W> operator,
          TupleTag<LeftT> leftTag,
          TupleTag<RightT> rightTag,
          AccumulatorProvider accProvider) {

    JoinFn<LeftT, RightT, K, OutputT> joinFn;
    BinaryFunctor<LeftT, RightT, OutputT> joiner = operator.getJoiner();
    String opName = operator.getName();

    switch (operator.getType()) {
      case INNER:
        joinFn = new InnerJoinFn<>(joiner, leftTag, rightTag, opName, accProvider);
        break;
      case LEFT:
        joinFn = new LeftOuterJoinFn<>(joiner, leftTag, rightTag, opName, accProvider);
        break;
      case RIGHT:
        joinFn = new RightOuterJoinFn<>(joiner, leftTag, rightTag, opName, accProvider);
        break;
      case FULL:
        joinFn = new FullJoinFn<>(joiner, leftTag, rightTag, opName, accProvider);
        break;

      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate Euphoria '%s' operator to Beam transformations."
                    + " Given join type '%s' is not supported.",
                Join.class.getSimpleName(), operator.getType()));
    }
    return joinFn;
  }
}
