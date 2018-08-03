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

import static org.apache.beam.sdk.extensions.euphoria.core.translate.common.OperatorTranslatorUtil.getKVInputCollection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join.Type;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.hint.SizeHint;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.windowing.WindowingDesc;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Translator for {@link org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin} and
 * {@link org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin} when one side of
 * the join fits in memory so it can be distributed in hashmap with the other side.
 */
public class BroadcastHashJoinTranslator implements OperatorTranslator<Join> {

  public static boolean hasFitsInMemoryHint(Operator operator) {
    return operator != null
        && operator.getHints() != null
        && operator.getHints().contains(SizeHint.FITS_IN_MEMORY);
  }

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(Join operator, TranslationContext context) {
    return doTranslate(operator, context);
  }

  <K, LeftT, RightT, OutputT, W extends BoundedWindow> PCollection<Pair<K, OutputT>> doTranslate(
      Join<LeftT, RightT, K, OutputT, W> operator, TranslationContext context) {
    Coder<K> keyCoder = context.getKeyCoder(operator);

    @SuppressWarnings("unchecked")
    final PCollection<LeftT> left = (PCollection<LeftT>) context.getInputs(operator).get(0);
    Coder<LeftT> leftCoder = context.getCoderBasedOnDatasetElementType(operator.getLeft());

    @SuppressWarnings("unchecked")
    final PCollection<RightT> right = (PCollection<RightT>) context.getInputs(operator).get(1);
    Coder<RightT> rightCoder = context.getCoderBasedOnDatasetElementType(operator.getRight());

    final PCollection<KV<K, LeftT>> leftKvInput =
        getKVInputCollection(
            left, operator.getLeftKeyExtractor(), keyCoder, leftCoder, ":extract-keys-left");

    final PCollection<KV<K, RightT>> rightKvInput =
        getKVInputCollection(
            right, operator.getRightKeyExtractor(), keyCoder, rightCoder, ":extract-keys-right");

    switch (operator.getType()) {
      case LEFT:
        final PCollectionView<Map<K, Iterable<RightT>>> broadcastRight =
            rightKvInput.apply(View.asMultimap());
        return leftKvInput.apply(
            ParDo.of(new BroadcastHashLeftJoinFn<>(broadcastRight, operator.getJoiner()))
                .withSideInputs(broadcastRight));

      case RIGHT:
        final PCollectionView<Map<K, Iterable<LeftT>>> broadcastLeft =
            leftKvInput.apply(View.asMultimap());
        return rightKvInput.apply(
            ParDo.of(new BroadcastHashRightJoinFn<>(broadcastLeft, operator.getJoiner()))
                .withSideInputs(broadcastLeft));

      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate Euphoria '%s' operator to Beam transformations."
                    + " Given join type '%s' is not supported for BrodcastHashJoin.",
                Join.class.getSimpleName(), operator.getType()));
    }
  }

  /**
   * Determines whenever given {@link Join} operator is of right type to be translated to
   * broadcasted hash join.
   */
  @Override
  public boolean canTranslate(Join operator) {
    @SuppressWarnings("unchecked")
    final ArrayList<Dataset> inputs = new ArrayList(operator.listInputs());
    if (inputs.size() != 2) {
      return false;
    }
    final Dataset leftDataset = inputs.get(0);
    final Dataset rightDataset = inputs.get(1);
    Type type = operator.getType();
    return ((type == Join.Type.LEFT && hasFitsInMemoryHint(rightDataset.getProducer()))
            || (type == Join.Type.RIGHT && hasFitsInMemoryHint(leftDataset.getProducer())))
        && isAllowedWindowing(operator.getWindowing());
  }

  /** BroadcastHashJoin supports only GlobalWindow or none. */
  private boolean isAllowedWindowing(WindowingDesc<?, ?> windowing) {
    return windowing == null || (windowing.getWindowFn() instanceof GlobalWindows);
  }

  static class BroadcastHashRightJoinFn<K, LeftT, RightT, OutputT>
      extends DoFn<KV<K, RightT>, Pair<K, OutputT>> {

    private final PCollectionView<Map<K, Iterable<LeftT>>> smallSideCollection;
    private final BinaryFunctor<LeftT, RightT, OutputT> joiner;
    private final SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    BroadcastHashRightJoinFn(
        PCollectionView<Map<K, Iterable<LeftT>>> smallSideCollection,
        BinaryFunctor<LeftT, RightT, OutputT> joiner) {
      this.smallSideCollection = smallSideCollection;
      this.joiner = joiner;
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(ProcessContext context) {
      final K key = context.element().getKey();
      final Map<K, Iterable<LeftT>> map = context.sideInput(smallSideCollection);
      final Iterable<LeftT> leftValues = map.getOrDefault(key, Collections.singletonList(null));
      leftValues.forEach(
          leftValue -> {
            joiner.apply(leftValue, context.element().getValue(), outCollector);
            context.output(Pair.of(key, outCollector.get()));
          });
    }
  }

  static class BroadcastHashLeftJoinFn<K, LeftT, RightT, OutputT>
      extends DoFn<KV<K, LeftT>, Pair<K, OutputT>> {

    private final PCollectionView<Map<K, Iterable<RightT>>> smallSideCollection;
    private final BinaryFunctor<LeftT, RightT, OutputT> joiner;
    private final SingleValueCollector<OutputT> outCollector = new SingleValueCollector<>();

    BroadcastHashLeftJoinFn(
        PCollectionView<Map<K, Iterable<RightT>>> smallSideCollection,
        BinaryFunctor<LeftT, RightT, OutputT> joiner) {
      this.smallSideCollection = smallSideCollection;
      this.joiner = joiner;
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(ProcessContext context) {
      final K key = context.element().getKey();
      final Map<K, Iterable<RightT>> map = context.sideInput(smallSideCollection);
      final Iterable<RightT> rightValues = map.getOrDefault(key, Collections.singletonList(null));

      rightValues.forEach(
          rightValue -> {
            joiner.apply(context.element().getValue(), rightValue, outCollector);
            context.output(Pair.of(key, outCollector.get()));
          });
    }
  }
}
