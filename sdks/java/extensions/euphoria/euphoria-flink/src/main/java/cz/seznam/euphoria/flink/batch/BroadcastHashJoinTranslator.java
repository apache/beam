/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.util.MultiValueContext;
import cz.seznam.euphoria.flink.FlinkOperator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Objects;

import static cz.seznam.euphoria.core.client.operator.hint.Util.wantTranslateBroadcastHashJoin;

public class BroadcastHashJoinTranslator implements BatchOperatorTranslator<Join> {


  static boolean wantTranslate(Join o) {
    return wantTranslateBroadcastHashJoin(o);
  }

  @Override
  @SuppressWarnings("unchecked")
  public DataSet<?> translate(FlinkOperator<Join> operator, BatchExecutorContext context) {
    final List<DataSet> inputs = (List) context.getInputStreams(operator);

    if (inputs.size() != 2) {
      throw new IllegalStateException(
          "Join should have two data sets on input, got " + inputs.size());
    }
    final DataSet left = inputs.get(0);
    final DataSet right = inputs.get(1);
    final Join originalOperator = operator.getOriginalOperator();

    final UnaryFunction leftKeyExtractor = originalOperator.getLeftKeyExtractor();
    final UnaryFunction rightKeyExtractor = originalOperator.getRightKeyExtractor();
    final Windowing windowing =
        originalOperator.getWindowing() == null
            ? AttachedWindowing.INSTANCE
            : originalOperator.getWindowing();
    DataSet<BatchElement<Window, Pair>> leftExtracted =
        left.flatMap(new KeyExtractor(leftKeyExtractor, windowing))
            .returns(new TypeHint<BatchElement<Window, Pair>>() {})
            .name(operator.getName() + "::extract-left-key")
            .setParallelism(operator.getParallelism());

    DataSet<BatchElement<Window, Pair>> rightExtracted =
        right.flatMap(new KeyExtractor(rightKeyExtractor, windowing))
            .returns(new TypeHint<BatchElement<Window, Pair>>() {})
            .name(operator.getName() + "::extract-right-key")
            .setParallelism(operator.getParallelism());

    DataSet<?> joined;
    switch (originalOperator.getType()) {
      case LEFT:
        joined = leftExtracted
            .leftOuterJoin(rightExtracted, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
            .where(new JoinKeySelector())
            .equalTo(new JoinKeySelector())
            .with(new BroadcastFlatJoinFunction(originalOperator.getJoiner()))
            .returns(new TypeHint<BatchElement<Window, Pair>>() {})
            .name(operator.getName() + "::left-broadcast-hash-join");

        break;
      case RIGHT:
        joined = leftExtracted
            .rightOuterJoin(rightExtracted, JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
            .where(new JoinKeySelector())
            .equalTo(new JoinKeySelector())
            .with(new BroadcastFlatJoinFunction(originalOperator.getJoiner()))
            .returns(new TypeHint<BatchElement<Window, Pair>>() {})
            .name(operator.getName() + "::right-broadcast-hash-join");
        break;
      default:
        throw new IllegalStateException("Invalid type: " + originalOperator.getType() + ".");
    }
    return joined;
  }

  private static class KeyExtractor
      implements FlatMapFunction<BatchElement, BatchElement<Window, Pair>> {

    private final UnaryFunction keyExtractor;
    private final Windowing windowing;

    KeyExtractor(UnaryFunction keyExtractor, Windowing windowing) {
      this.keyExtractor = keyExtractor;
      this.windowing = windowing;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void flatMap(BatchElement wel, Collector<BatchElement<Window, Pair>> coll) throws Exception {
      Iterable<Window> assigned = windowing.assignWindowsToElement(wel);
      for (Window wid : assigned) {
        Object el = wel.getElement();
        coll.collect(new BatchElement(
            wid, wid.maxTimestamp(), Pair.of(keyExtractor.apply(el), el)));
      }
    }
  }

  static class BroadcastFlatJoinFunction
      implements FlatJoinFunction<BatchElement<Window, Pair>, BatchElement<Window, Pair>,
      BatchElement<Window, Pair>> {
    final BinaryFunctor<Object, Object, Object> joiner;
    transient MultiValueContext<Object> multiValueContext;

    BroadcastFlatJoinFunction(BinaryFunctor<Object, Object, Object> joiner) {
      this.joiner = joiner;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void join(BatchElement<Window, Pair> first, BatchElement<Window, Pair> second,
                     Collector<BatchElement<Window, Pair>> coll) throws Exception {

      if (multiValueContext == null) {
        multiValueContext = new MultiValueContext<>();
      }
      final Window window = first == null ? second.getWindow() : first.getWindow();

      final long maxTimestamp = Math.max(
          first == null ? window.maxTimestamp() - 1 : first.getTimestamp(),
          second == null ? window.maxTimestamp() - 1 : second.getTimestamp());

      Object firstEl = first == null ? null : first.getElement().getSecond();
      Object secondEl = second == null ? null : second.getElement().getSecond();

      joiner.apply(firstEl, secondEl, multiValueContext);

      final Object key = first == null
          ? second.getElement().getFirst()
          : first.getElement().getFirst();
      List<Object> values = multiValueContext.getAndResetValues();
      values.forEach(val -> coll.collect(new BatchElement<>(
          window,
          maxTimestamp,
          Pair.of(key, val))));
    }
  }

  static class JoinKeySelector
      implements KeySelector<BatchElement<Window, Pair>, KeyedWindow<Window, Object>> {

    @Override
    public KeyedWindow<Window, Object> getKey(BatchElement<Window, Pair> value) {
      return new KeyedWindow<Window, Object>(value.getWindow(), value.getElement().getFirst());
    }

  }

  public static final class KeyedWindow<W extends Window, K> implements Comparable<KeyedWindow> {
    private final W window;
    private final K key;

    KeyedWindow(W window, K key) {
      this.window = Objects.requireNonNull(window);
      this.key = Objects.requireNonNull(key);
    }

    public W window() {
      return window;
    }

    public K key() {
      return key;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof KeyedWindow) {
        final KeyedWindow other = (KeyedWindow) o;
        return Objects.equals(window, other.window) && Objects.equals(key, other.key);
      }
      return false;
    }

    @Override
    public int hashCode() {
      int result = window.hashCode();
      result = 31 * result + (key != null ? key.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "KeyedWindow{" +
          "window=" + window +
          ", key=" + key +
          '}';
    }

    @Override
    public int compareTo(KeyedWindow other) {
      final int compareWindowResult = this.window.compareTo(other.window);
      if (compareWindowResult == 0) {
        if (Objects.equals(key, other.key)) {
          return 0;
        } else {
          return 1;
        }
      }
      return compareWindowResult;
    }
  }
}


