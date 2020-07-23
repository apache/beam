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

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.translate.collector.AdaptableCollector;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Table;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Translator for {@link org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin} and
 * {@link org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin} when one side of
 * the join fits in memory so it can be distributed in hash map with the other side.
 *
 * <p>Note that when reusing smaller join side to several broadcast hash joins there are some rules
 * to follow to avoid data to be send to executors repeatedly:
 *
 * <ul>
 *   <li>Input {@link PCollection} of broadcast side has to be the same instance
 *   <li>Key extractor of broadcast side has to be the same {@link UnaryFunction} instance
 * </ul>
 */
public class BroadcastHashJoinTranslator<LeftT, RightT, KeyT, OutputT>
    extends AbstractJoinTranslator<LeftT, RightT, KeyT, OutputT> {

  /**
   * Used to prevent multiple views to the same input PCollection. And therefore multiple broadcasts
   * of the same data.
   */
  @VisibleForTesting
  final Table<PCollection<?>, UnaryFunction<?, KeyT>, PCollectionView<?>> pViews =
      HashBasedTable.create();

  @Override
  PCollection<KV<KeyT, OutputT>> translate(
      Join<LeftT, RightT, KeyT, OutputT> operator,
      PCollection<LeftT> left,
      PCollection<KV<KeyT, LeftT>> leftKeyed,
      PCollection<RightT> right,
      PCollection<KV<KeyT, RightT>> rightKeyed) {

    final AccumulatorProvider accumulators =
        new LazyAccumulatorProvider(AccumulatorProvider.of(left.getPipeline()));

    switch (operator.getType()) {
      case LEFT:
        final PCollectionView<Map<KeyT, Iterable<RightT>>> broadcastRight =
            computeViewAsMultimapIfAbsent(right, operator.getRightKeyExtractor(), rightKeyed);
        return leftKeyed.apply(
            ParDo.of(
                    new BroadcastHashLeftJoinFn<>(
                        broadcastRight,
                        operator.getJoiner(),
                        accumulators,
                        operator.getName().orElse(null)))
                .withSideInputs(broadcastRight));
      case RIGHT:
        final PCollectionView<Map<KeyT, Iterable<LeftT>>> broadcastLeft =
            computeViewAsMultimapIfAbsent(left, operator.getLeftKeyExtractor(), leftKeyed);
        return rightKeyed.apply(
            ParDo.of(
                    new BroadcastHashRightJoinFn<>(
                        broadcastLeft,
                        operator.getJoiner(),
                        accumulators,
                        operator.getName().orElse(null)))
                .withSideInputs(broadcastLeft));
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate Euphoria '%s' operator to Beam transformations."
                    + " Given join type '%s' is not supported for BroadcastHashJoin.",
                Join.class.getSimpleName(), operator.getType()));
    }
  }

  /**
   * Creates new {@link PCollectionView} of given {@code pCollectionToView} iff there is no {@link
   * PCollectionView} already associated with {@code Key}.
   *
   * @param pCollectionToView a {@link PCollection} view will be created from by applying {@link
   *     View#asMultimap()}
   * @param <V> value key type
   * @return the current (already existing or computed) value associated with the specified key
   */
  private <V> PCollectionView<Map<KeyT, Iterable<V>>> computeViewAsMultimapIfAbsent(
      PCollection<V> pcollection,
      UnaryFunction<?, KeyT> keyExtractor,
      final PCollection<KV<KeyT, V>> pCollectionToView) {

    PCollectionView<?> view = pViews.get(pcollection, keyExtractor);
    if (view == null) {
      view = pCollectionToView.apply(View.asMultimap());
      pViews.put(pcollection, keyExtractor, view);
    }

    @SuppressWarnings("unchecked")
    PCollectionView<Map<KeyT, Iterable<V>>> ret = (PCollectionView<Map<KeyT, Iterable<V>>>) view;
    return ret;
  }

  static class BroadcastHashRightJoinFn<K, LeftT, RightT, OutputT>
      extends DoFn<KV<K, RightT>, KV<K, OutputT>> {

    private final PCollectionView<Map<K, Iterable<LeftT>>> smallSideCollection;
    private final BinaryFunctor<LeftT, RightT, OutputT> joiner;
    private final AdaptableCollector<KV<K, RightT>, KV<K, OutputT>, OutputT> outCollector;

    BroadcastHashRightJoinFn(
        PCollectionView<Map<K, Iterable<LeftT>>> smallSideCollection,
        BinaryFunctor<LeftT, RightT, OutputT> joiner,
        AccumulatorProvider accumulators,
        @Nullable String operatorName) {
      this.smallSideCollection = smallSideCollection;
      this.joiner = joiner;
      this.outCollector =
          new AdaptableCollector<>(
              accumulators,
              operatorName,
              (ctx, elem) -> ctx.output(KV.of(ctx.element().getKey(), elem)));
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(ProcessContext context) {
      final KV<K, RightT> element = context.element();
      final K key = element.getKey();
      final Map<K, Iterable<LeftT>> map = context.sideInput(smallSideCollection);
      final Iterable<LeftT> leftValues = map.getOrDefault(key, Collections.singletonList(null));
      outCollector.setProcessContext(context);
      leftValues.forEach(leftValue -> joiner.apply(leftValue, element.getValue(), outCollector));
    }
  }

  static class BroadcastHashLeftJoinFn<K, LeftT, RightT, OutputT>
      extends DoFn<KV<K, LeftT>, KV<K, OutputT>> {

    private final PCollectionView<Map<K, Iterable<RightT>>> smallSideCollection;
    private final BinaryFunctor<LeftT, RightT, OutputT> joiner;
    private final AdaptableCollector<KV<K, LeftT>, KV<K, OutputT>, OutputT> outCollector;

    BroadcastHashLeftJoinFn(
        PCollectionView<Map<K, Iterable<RightT>>> smallSideCollection,
        BinaryFunctor<LeftT, RightT, OutputT> joiner,
        AccumulatorProvider accumulators,
        @Nullable String operatorName) {
      this.smallSideCollection = smallSideCollection;
      this.joiner = joiner;
      this.outCollector =
          new AdaptableCollector<>(
              accumulators,
              operatorName,
              (ctx, elem) -> ctx.output(KV.of(ctx.element().getKey(), elem)));
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(ProcessContext context) {
      final KV<K, LeftT> element = context.element();
      final K key = element.getKey();
      final Map<K, Iterable<RightT>> map = context.sideInput(smallSideCollection);
      final Iterable<RightT> rightValues = map.getOrDefault(key, Collections.singletonList(null));
      outCollector.setProcessContext(context);
      rightValues.forEach(rightValue -> joiner.apply(element.getValue(), rightValue, outCollector));
    }
  }
}
