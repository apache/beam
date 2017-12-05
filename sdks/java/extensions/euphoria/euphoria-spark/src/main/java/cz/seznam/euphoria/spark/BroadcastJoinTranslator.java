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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.Join;
import cz.seznam.euphoria.core.client.util.Either;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BroadcastJoinTranslator implements SparkOperatorTranslator<Join> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(Join operator, SparkExecutorContext context) {

    // ~ sanity check
    Preconditions.checkArgument(
        operator.getHints().contains(JoinHints.broadcastHashJoin()),
        "Missing broadcastHashJoin hint");
    Preconditions.checkArgument(
        operator.getType() == Join.Type.LEFT || operator.getType() == Join.Type.RIGHT,
        "BroadcastJoin supports LEFT and RIGHT joins only");

    final List<JavaRDD<?>> inputs = context.getInputs(operator);
    final JavaRDD<SparkElement> left = (JavaRDD<SparkElement>) inputs.get(0);
    final JavaRDD<SparkElement> right = (JavaRDD<SparkElement>) inputs.get(1);

    final Windowing windowing = operator.getWindowing() == null
        ? AttachedWindowing.INSTANCE
        : operator.getWindowing();

    final JavaPairRDD<KeyedWindow, SparkElement> leftPair =
        left.flatMapToPair(new KeyExtractor(operator.getLeftKeyExtractor(), windowing, true));
    final JavaPairRDD<KeyedWindow, SparkElement> rightPair =
        right.flatMapToPair(new KeyExtractor(operator.getRightKeyExtractor(), windowing, false));

    final JavaPairRDD<KeyedWindow, Tuple2<Optional<SparkElement>, Optional<SparkElement>>> joined;

    switch (operator.getType()) {
      case LEFT: {

        // ~ this will submit a new job, in order to collect results
        final Broadcast<Map<KeyedWindow, List<SparkElement>>> broadcast = context
            .getExecutionEnvironment()
            .broadcast(toBroadcast(rightPair.collect()));

        joined = leftPair.flatMapToPair(t -> {
          if (broadcast.getValue().containsKey(t._1)) {
            return Iterables.transform(broadcast.getValue().get(t._1), (el) ->
                new Tuple2<>(t._1, new Tuple2<>(opt(t._2), opt(el)))).iterator();
          } else {
            return Collections.singletonList(
                new Tuple2<>(t._1,
                    new Tuple2<>(opt(t._2), Optional.<SparkElement>empty()))).iterator();
          }
        });

        break;
      }
      case RIGHT: {

        // ~ this will submit a new job, in order to collect results
        final Broadcast<Map<KeyedWindow, List<SparkElement>>> broadcast = context
            .getExecutionEnvironment()
            .broadcast(toBroadcast(leftPair.collect()));

        joined = rightPair.flatMapToPair(t -> {
          if (broadcast.getValue().containsKey(t._1)) {
            return Iterables.transform(broadcast.getValue().get(t._1), (el) ->
                new Tuple2<>(t._1, new Tuple2<>(opt(el), opt(t._2)))).iterator();
          } else {
            return Collections.singletonList(
                new Tuple2<>(t._1,
                    new Tuple2<>(Optional.<SparkElement>empty(), opt(t._2)))).iterator();
          }
        });

        break;
      }
      default: {
        throw new IllegalStateException("Invalid type: " + operator.getType() + ".");
      }
    }

    return joined.flatMap(new FlatMapFunctionWithCollector<>((t, collector) -> {
      // ~ both elements have exactly the same window
      // we need to check for null values because of full outer join
      final SparkElement first = t._2._1.orNull();
      final SparkElement second = t._2._2.orNull();
      final Window window = first == null ? second.getWindow() : first.getWindow();
      final long maxTimestamp = Math.max(
          first == null ? 0L : first.getTimestamp(),
          second == null ? 0L : second.getTimestamp());
      collector.clear();
      collector.setWindow(window);
      operator.getJoiner().apply(
          first == null ? null : first.getElement(),
          second == null ? null : second.getElement(),
          collector);
      return Iterators.transform(collector.getOutputIterator(), e ->
          new SparkElement<>(window, maxTimestamp, Pair.of(t._1.key(), e)));
    }, new LazyAccumulatorProvider(context.getAccumulatorFactory(), context.getSettings())));
  }

  private static <T> Optional<T> opt(T val) {
    return Optional.ofNullable(val);
  }

  private static Map<KeyedWindow, List<SparkElement>> toBroadcast(
      List<Tuple2<KeyedWindow, SparkElement>> list) {
    final Map<KeyedWindow, List<SparkElement>> res = new HashMap<>();
    list.forEach((t) -> {
      final List<SparkElement> elements = res.computeIfAbsent(t._1, (k)
          -> new ArrayList<>());
      elements.add(t._2);
    });
    return res;
  }

  private static class KeyExtractor implements PairFlatMapFunction<SparkElement, KeyedWindow,
      SparkElement> {

    private final UnaryFunction keyExtractor;
    private final Windowing windowing;
    private final boolean left;

    KeyExtractor(UnaryFunction keyExtractor, Windowing windowing, boolean left) {
      this.keyExtractor = keyExtractor;
      this.windowing = windowing;
      this.left = left;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<KeyedWindow, SparkElement>> call(SparkElement se) throws Exception {
      final Iterable<Window> windows = windowing.assignWindowsToElement(new SparkElement(se
          .getWindow(),
          se.getTimestamp(), left ? Either.left(se.getElement()) : Either.right(se.getElement())));
      return Iterators.transform(windows.iterator(), (w) -> new Tuple2<>(
          new KeyedWindow<>(w, se.getTimestamp(), keyExtractor.apply(se.getElement())),
          new SparkElement(w, se.getTimestamp(), se.getElement())));
    }
  }
}
