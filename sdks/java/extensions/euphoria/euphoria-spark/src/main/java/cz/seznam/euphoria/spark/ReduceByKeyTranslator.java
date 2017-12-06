/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.shadow.com.google.common.collect.Iterators;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.ReduceFunctor;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.util.SingleValueContext;
import cz.seznam.euphoria.shadow.com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

class ReduceByKeyTranslator implements SparkOperatorTranslator<ReduceByKey> {

  static boolean wantTranslate(ReduceByKey operator) {
    return operator.isCombinable()
            && (operator.getWindowing() == null
                || (!(operator.getWindowing() instanceof MergingWindowing)
                    && !operator.getWindowing().getTrigger().isStateful()));
  }

  @Override
  public JavaRDD<?> translate(ReduceByKey operator, SparkExecutorContext context) {
    @SuppressWarnings("unchecked")
    final JavaRDD<SparkElement> input = (JavaRDD<SparkElement>) context.getSingleInput(operator);
    @SuppressWarnings("unchecked")
    final ReduceFunctor<Iterable<Object>, Object> reducer = operator.getReducer();

    final Windowing windowing =
            operator.getWindowing() == null
                    ? AttachedWindowing.INSTANCE
                    : operator.getWindowing();
    final UnaryFunction keyExtractor = operator.getKeyExtractor();
    final UnaryFunction valueExtractor = operator.getValueExtractor();

    Preconditions.checkState(operator.isCombinable(),
            "Non-combinable ReduceByKey not supported!");
    Preconditions.checkState(
            !(windowing instanceof MergingWindowing),
            "MergingWindowing not supported!");
    Preconditions.checkState(!windowing.getTrigger().isStateful(),
            "Stateful triggers not supported!");

    // ~ extract key/value + timestamp from input elements and assign windows
    JavaPairRDD<KeyedWindow, TimestampedElement> tuples = input.flatMapToPair(
            new CompositeKeyExtractor(keyExtractor, valueExtractor, windowing));

    JavaPairRDD<KeyedWindow, TimestampedElement> reduced;
    reduced = tuples.reduceByKey(new Reducer(reducer));

    return reduced.map(t -> {
      KeyedWindow kw = t._1();
      TimestampedElement el = t._2();

      // ~ extract timestamp from element rather than from KeyedWindow
      // because in KeyedWindow there is the original timestamp from
      // pre-reduce age
      long timestamp = el.getTimestamp();

      return new SparkElement<>(
              kw.window(), timestamp, Pair.of(kw.key(), el.getElement()));
    });
  }

  /**
   * Extracts {@link KeyedWindow} from {@link SparkElement} and
   * assigns timestamp according to (optional) eventTimeAssigner.
   */
  private static class CompositeKeyExtractor
          implements PairFlatMapFunction<SparkElement, KeyedWindow, TimestampedElement> {

    private final UnaryFunction keyExtractor;
    private final UnaryFunction valueExtractor;
    private final Windowing windowing;

    CompositeKeyExtractor(UnaryFunction keyExtractor,
                                 UnaryFunction valueExtractor,
                                 Windowing windowing) {
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.windowing = windowing;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<KeyedWindow, TimestampedElement>> call(SparkElement wel) throws Exception {
      final Iterable<Window> windows = windowing.assignWindowsToElement(wel);
      return Iterators.transform(windows.iterator(), wid -> {
        final long stamp = Objects.requireNonNull(wid).maxTimestamp() - 1;
        return new Tuple2<>(
            new KeyedWindow<>(wid, stamp, keyExtractor.apply(wel.getElement())),
            new TimestampedElement(stamp, valueExtractor.apply(wel.getElement())));
      });
    }
  }

  private static class Reducer implements
          Function2<TimestampedElement, TimestampedElement, TimestampedElement> {

    private final ReduceFunctor<Iterable<Object>, Object> reducer;

    // cached array to avoid repeated allocation
    private Object[] iterable;
    private SingleValueContext<Object> context;

    private Reducer(ReduceFunctor<Iterable<Object>, Object> reducer) {
      this.reducer = reducer;
      this.iterable = new Object[2];
    }

    @SuppressWarnings("unchecked")
    @Override
    public TimestampedElement call(TimestampedElement o1, TimestampedElement o2) {
      if (context == null) {
        context = new SingleValueContext<>();
      }
      iterable[0] = o1.getElement();
      iterable[1] = o2.getElement();
      reducer.apply((Stream) Stream.of(iterable), context);

      return new TimestampedElement(
          Math.max(o1.getTimestamp(), o2.getTimestamp()),
          context.getAndResetValue());
    }
  }
}
