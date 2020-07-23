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

import static java.util.Objects.requireNonNull;

import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.extensions.euphoria.core.translate.collector.AdaptableCollector;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

/** {@link OperatorTranslator Translator } for Euphoria {@link Join} operator. */
public class JoinTranslator<LeftT, RightT, KeyT, OutputT>
    extends AbstractJoinTranslator<LeftT, RightT, KeyT, OutputT> {

  private abstract static class JoinFn<LeftT, RightT, KeyT, OutputT>
      extends DoFn<KV<KeyT, CoGbkResult>, KV<KeyT, OutputT>> {

    private final BinaryFunctor<LeftT, RightT, OutputT> joiner;
    private final TupleTag<LeftT> leftTag;
    private final TupleTag<RightT> rightTag;

    private final AdaptableCollector<KV<KeyT, CoGbkResult>, KV<KeyT, OutputT>, OutputT>
        resultsCollector;

    JoinFn(
        BinaryFunctor<LeftT, RightT, OutputT> joiner,
        TupleTag<LeftT> leftTag,
        TupleTag<RightT> rightTag,
        @Nullable String operatorName,
        AccumulatorProvider accumulatorProvider) {
      this.joiner = joiner;
      this.leftTag = leftTag;
      this.rightTag = rightTag;
      this.resultsCollector =
          new AdaptableCollector<>(
              accumulatorProvider,
              operatorName,
              ((ctx, elem) -> ctx.output(KV.of(ctx.element().getKey(), elem))));
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public final void processElement(@Element KV<KeyT, CoGbkResult> element, ProcessContext ctx) {
      getCollector().setProcessContext(ctx);
      doJoin(
          requireNonNull(element.getValue()).getAll(leftTag),
          requireNonNull(element.getValue()).getAll(rightTag));
    }

    abstract void doJoin(Iterable<LeftT> left, Iterable<RightT> right);

    abstract String getFnName();

    BinaryFunctor<LeftT, RightT, OutputT> getJoiner() {
      return joiner;
    }

    AdaptableCollector<KV<KeyT, CoGbkResult>, KV<KeyT, OutputT>, OutputT> getCollector() {
      return resultsCollector;
    }
  }

  private static class InnerJoinFn<LeftT, RightT, KeyT, OutputT>
      extends JoinFn<LeftT, RightT, KeyT, OutputT> {

    InnerJoinFn(
        BinaryFunctor<LeftT, RightT, OutputT> joiner,
        TupleTag<LeftT> leftTag,
        TupleTag<RightT> rightTag,
        @Nullable String operatorName,
        AccumulatorProvider accumulatorProvider) {
      super(joiner, leftTag, rightTag, operatorName, accumulatorProvider);
    }

    @Override
    protected void doJoin(Iterable<LeftT> left, Iterable<RightT> right) {
      for (LeftT leftItem : left) {
        for (RightT rightItem : right) {
          getJoiner().apply(leftItem, rightItem, getCollector());
        }
      }
    }

    @Override
    String getFnName() {
      return "inner-join";
    }
  }

  private static class FullJoinFn<LeftT, RightT, K, OutputT>
      extends JoinFn<LeftT, RightT, K, OutputT> {

    FullJoinFn(
        BinaryFunctor<LeftT, RightT, OutputT> joiner,
        TupleTag<LeftT> leftTag,
        TupleTag<RightT> rightTag,
        @Nullable String operatorName,
        AccumulatorProvider accumulatorProvider) {
      super(joiner, leftTag, rightTag, operatorName, accumulatorProvider);
    }

    @Override
    void doJoin(Iterable<LeftT> left, Iterable<RightT> right) {
      final boolean leftHasValues = left.iterator().hasNext();
      final boolean rightHasValues = right.iterator().hasNext();
      if (leftHasValues && rightHasValues) {
        for (RightT rightValue : right) {
          for (LeftT leftValue : left) {
            getJoiner().apply(leftValue, rightValue, getCollector());
          }
        }
      } else if (leftHasValues) {
        for (LeftT leftValue : left) {
          getJoiner().apply(leftValue, null, getCollector());
        }
      } else if (rightHasValues) {
        for (RightT rightValue : right) {
          getJoiner().apply(null, rightValue, getCollector());
        }
      }
    }

    @Override
    public String getFnName() {
      return "full-join";
    }
  }

  private static class LeftOuterJoinFn<LeftT, RightT, K, OutputT>
      extends JoinFn<LeftT, RightT, K, OutputT> {

    LeftOuterJoinFn(
        BinaryFunctor<LeftT, RightT, OutputT> joiner,
        TupleTag<LeftT> leftTag,
        TupleTag<RightT> rightTag,
        @Nullable String operatorName,
        AccumulatorProvider accumulatorProvider) {
      super(joiner, leftTag, rightTag, operatorName, accumulatorProvider);
    }

    @Override
    void doJoin(Iterable<LeftT> left, Iterable<RightT> right) {
      for (LeftT leftValue : left) {
        if (right.iterator().hasNext()) {
          for (RightT rightValue : right) {
            getJoiner().apply(leftValue, rightValue, getCollector());
          }
        } else {
          getJoiner().apply(leftValue, null, getCollector());
        }
      }
    }

    @Override
    public String getFnName() {
      return "left-outer-join";
    }
  }

  private static class RightOuterJoinFn<LeftT, RightT, K, OutputT>
      extends JoinFn<LeftT, RightT, K, OutputT> {

    RightOuterJoinFn(
        BinaryFunctor<LeftT, RightT, OutputT> joiner,
        TupleTag<LeftT> leftTag,
        TupleTag<RightT> rightTag,
        @Nullable String operatorName,
        AccumulatorProvider accumulatorProvider) {
      super(joiner, leftTag, rightTag, operatorName, accumulatorProvider);
    }

    @Override
    void doJoin(Iterable<LeftT> left, Iterable<RightT> right) {
      for (RightT rightValue : right) {
        if (left.iterator().hasNext()) {
          for (LeftT leftValue : left) {
            getJoiner().apply(leftValue, rightValue, getCollector());
          }
        } else {
          getJoiner().apply(null, rightValue, getCollector());
        }
      }
    }

    @Override
    public String getFnName() {
      return "::right-outer-join";
    }
  }

  private static <KeyT, LeftT, RightT, OutputT> JoinFn<LeftT, RightT, KeyT, OutputT> getJoinFn(
      Join<LeftT, RightT, KeyT, OutputT> operator,
      TupleTag<LeftT> leftTag,
      TupleTag<RightT> rightTag,
      AccumulatorProvider accumulators) {
    final BinaryFunctor<LeftT, RightT, OutputT> joiner = operator.getJoiner();
    switch (operator.getType()) {
      case INNER:
        return new InnerJoinFn<>(
            joiner, leftTag, rightTag, operator.getName().orElse(null), accumulators);
      case LEFT:
        return new LeftOuterJoinFn<>(
            joiner, leftTag, rightTag, operator.getName().orElse(null), accumulators);
      case RIGHT:
        return new RightOuterJoinFn<>(
            joiner, leftTag, rightTag, operator.getName().orElse(null), accumulators);
      case FULL:
        return new FullJoinFn<>(
            joiner, leftTag, rightTag, operator.getName().orElse(null), accumulators);
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot translate Euphoria '%s' operator to Beam transformations."
                    + " Given join type '%s' is not supported.",
                Join.class.getSimpleName(), operator.getType()));
    }
  }

  @Override
  PCollection<KV<KeyT, OutputT>> translate(
      Join<LeftT, RightT, KeyT, OutputT> operator,
      PCollection<LeftT> left,
      PCollection<KV<KeyT, LeftT>> leftKeyed,
      PCollection<RightT> reight,
      PCollection<KV<KeyT, RightT>> rightKeyed) {
    final AccumulatorProvider accumulators =
        new LazyAccumulatorProvider(AccumulatorProvider.of(leftKeyed.getPipeline()));
    final TupleTag<LeftT> leftTag = new TupleTag<>();
    final TupleTag<RightT> rightTag = new TupleTag<>();
    final JoinFn<LeftT, RightT, KeyT, OutputT> joinFn =
        getJoinFn(operator, leftTag, rightTag, accumulators);
    return KeyedPCollectionTuple.of(leftTag, leftKeyed)
        .and(rightTag, rightKeyed)
        .apply("co-group-by-key", CoGroupByKey.create())
        .apply(joinFn.getFnName(), ParDo.of(joinFn));
  }
}
