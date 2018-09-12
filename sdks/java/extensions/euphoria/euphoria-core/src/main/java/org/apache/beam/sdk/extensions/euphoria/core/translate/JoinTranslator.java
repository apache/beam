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

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Join;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/** {@link OperatorTranslator Translator } for Euphoria {@link Join} operator. */
public class JoinTranslator<LeftT, RightT, KeyT, OutputT>
    extends AbstractJoinTranslator<LeftT, RightT, KeyT, OutputT> {

  private abstract static class JoinFn<LeftT, RightT, KeyT, OutputT>
      extends DoFn<KV<KeyT, CoGbkResult>, KV<KeyT, OutputT>> {

    private final BinaryFunctor<LeftT, RightT, OutputT> joiner;
    private final TupleTag<LeftT> leftTag;
    private final TupleTag<RightT> rightTag;

    private transient SingleValueCollector<OutputT> collector;

    JoinFn(
        BinaryFunctor<LeftT, RightT, OutputT> joiner,
        TupleTag<LeftT> leftTag,
        TupleTag<RightT> rightTag) {
      this.joiner = joiner;
      this.leftTag = leftTag;
      this.rightTag = rightTag;
    }

    @ProcessElement
    @SuppressWarnings("unused")
    public final void processElement(
        @Element KV<KeyT, CoGbkResult> element, OutputReceiver<KV<KeyT, OutputT>> outputReceiver) {
      doJoin(
          element.getKey(),
          requireNonNull(element.getValue()).getAll(leftTag),
          requireNonNull(element.getValue()).getAll(rightTag),
          outputReceiver);
    }

    abstract void doJoin(
        KeyT key,
        Iterable<LeftT> left,
        Iterable<RightT> right,
        OutputReceiver<KV<KeyT, OutputT>> outputReceiver);

    abstract String getFnName();

    BinaryFunctor<LeftT, RightT, OutputT> getJoiner() {
      return joiner;
    }

    SingleValueCollector<OutputT> getCollector() {
      if (collector == null) {
        collector = new SingleValueCollector<>();
      }
      return collector;
    }
  }

  private static class InnerJoinFn<LeftT, RightT, KeyT, OutputT>
      extends JoinFn<LeftT, RightT, KeyT, OutputT> {

    InnerJoinFn(
        BinaryFunctor<LeftT, RightT, OutputT> functor,
        TupleTag<LeftT> leftTag,
        TupleTag<RightT> rightTag) {
      super(functor, leftTag, rightTag);
    }

    @Override
    protected void doJoin(
        KeyT key,
        Iterable<LeftT> left,
        Iterable<RightT> right,
        OutputReceiver<KV<KeyT, OutputT>> outputReceiver) {
      final SingleValueCollector<OutputT> coll = getCollector();
      for (LeftT leftItem : left) {
        for (RightT rightItem : right) {
          getJoiner().apply(leftItem, rightItem, coll);
          outputReceiver.output(KV.of(key, coll.get()));
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
        TupleTag<RightT> rightTag) {
      super(joiner, leftTag, rightTag);
    }

    @Override
    void doJoin(
        K key,
        Iterable<LeftT> left,
        Iterable<RightT> right,
        OutputReceiver<KV<K, OutputT>> outputReceiver) {
      final boolean leftHasValues = left.iterator().hasNext();
      final boolean rightHasValues = right.iterator().hasNext();
      final SingleValueCollector<OutputT> coll = getCollector();
      if (leftHasValues && rightHasValues) {
        for (RightT rightValue : right) {
          for (LeftT leftValue : left) {
            getJoiner().apply(leftValue, rightValue, coll);
            outputReceiver.output(KV.of(key, coll.get()));
          }
        }
      } else if (leftHasValues) {
        for (LeftT leftValue : left) {
          getJoiner().apply(leftValue, null, coll);
          outputReceiver.output(KV.of(key, coll.get()));
        }
      } else if (rightHasValues) {
        for (RightT rightValue : right) {
          getJoiner().apply(null, rightValue, coll);
          outputReceiver.output(KV.of(key, coll.get()));
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
        TupleTag<RightT> rightTag) {
      super(joiner, leftTag, rightTag);
    }

    @Override
    void doJoin(
        K key,
        Iterable<LeftT> left,
        Iterable<RightT> right,
        OutputReceiver<KV<K, OutputT>> outputReceiver) {
      final SingleValueCollector<OutputT> coll = getCollector();
      for (LeftT leftValue : left) {
        if (right.iterator().hasNext()) {
          for (RightT rightValue : right) {
            getJoiner().apply(leftValue, rightValue, coll);
            outputReceiver.output(KV.of(key, coll.get()));
          }
        } else {
          getJoiner().apply(leftValue, null, coll);
          outputReceiver.output(KV.of(key, coll.get()));
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
        TupleTag<RightT> rightTag) {
      super(joiner, leftTag, rightTag);
    }

    @Override
    void doJoin(
        K key,
        Iterable<LeftT> left,
        Iterable<RightT> right,
        OutputReceiver<KV<K, OutputT>> outputReceiver) {
      final SingleValueCollector<OutputT> coll = new SingleValueCollector<>();

      for (RightT rightValue : right) {
        if (left.iterator().hasNext()) {
          for (LeftT leftValue : left) {
            getJoiner().apply(leftValue, rightValue, coll);
            outputReceiver.output(KV.of(key, coll.get()));
          }
        } else {
          getJoiner().apply(null, rightValue, coll);
          outputReceiver.output(KV.of(key, coll.get()));
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
      TupleTag<RightT> rightTag) {
    final BinaryFunctor<LeftT, RightT, OutputT> joiner = operator.getJoiner();
    switch (operator.getType()) {
      case INNER:
        return new InnerJoinFn<>(joiner, leftTag, rightTag);
      case LEFT:
        return new LeftOuterJoinFn<>(joiner, leftTag, rightTag);
      case RIGHT:
        return new RightOuterJoinFn<>(joiner, leftTag, rightTag);
      case FULL:
        return new FullJoinFn<>(joiner, leftTag, rightTag);
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
      PCollection<KV<KeyT, LeftT>> left,
      PCollection<KV<KeyT, RightT>> right) {
    final TupleTag<LeftT> leftTag = new TupleTag<>();
    final TupleTag<RightT> rightTag = new TupleTag<>();
    final JoinFn<LeftT, RightT, KeyT, OutputT> joinFn = getJoinFn(operator, leftTag, rightTag);
    return KeyedPCollectionTuple.of(leftTag, left)
        .and(rightTag, right)
        .apply("co-group-by-key", CoGroupByKey.create())
        .apply(joinFn.getFnName(), ParDo.of(joinFn));
  }
}
