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
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Translator for {@link org.apache.beam.sdk.extensions.euphoria.core.client.operator.RightJoin} and
 * {@link org.apache.beam.sdk.extensions.euphoria.core.client.operator.LeftJoin} when one side of
 * the join fits in memory so it can be distributed in hash map with the other side.
 */
// TODO vaskovo fixy
public class BroadcastHashJoinTranslator<LeftT, RightT, KeyT, OutputT>
    extends AbstractJoinTranslator<LeftT, RightT, KeyT, OutputT> {

  @Override
  PCollection<KV<KeyT, OutputT>> translate(
      Join<LeftT, RightT, KeyT, OutputT> operator,
      PCollection<KV<KeyT, LeftT>> left,
      PCollection<KV<KeyT, RightT>> right) {
    switch (operator.getType()) {
      case LEFT:
        final PCollectionView<Map<KeyT, Iterable<RightT>>> broadcastRight =
            right.apply(View.asMultimap());
        return left.apply(
            ParDo.of(new BroadcastHashLeftJoinFn<>(broadcastRight, operator.getJoiner()))
                .withSideInputs(broadcastRight));
      case RIGHT:
        final PCollectionView<Map<KeyT, Iterable<LeftT>>> broadcastLeft =
            left.apply(View.asMultimap());
        return right.apply(
            ParDo.of(new BroadcastHashRightJoinFn<>(broadcastLeft, operator.getJoiner()))
                .withSideInputs(broadcastLeft));
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate Euphoria '%s' operator to Beam transformations."
                    + " Given join type '%s' is not supported for BroadcastHashJoin.",
                Join.class.getSimpleName(), operator.getType()));
    }
  }

  static class BroadcastHashRightJoinFn<K, LeftT, RightT, OutputT>
      extends DoFn<KV<K, RightT>, KV<K, OutputT>> {

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
            context.output(KV.of(key, outCollector.get()));
          });
    }
  }

  static class BroadcastHashLeftJoinFn<K, LeftT, RightT, OutputT>
      extends DoFn<KV<K, LeftT>, KV<K, OutputT>> {

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
            context.output(KV.of(key, outCollector.get()));
          });
    }
  }
}
