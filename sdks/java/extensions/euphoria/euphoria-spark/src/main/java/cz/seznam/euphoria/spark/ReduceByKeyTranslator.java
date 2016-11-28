package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

class ReduceByKeyTranslator implements SparkOperatorTranslator<ReduceByKey> {

  static boolean wantTranslate(ReduceByKey operator) {
    boolean b = operator.isCombinable()
            && (operator.getWindowing() == null
                || (!(operator.getWindowing() instanceof MergingWindowing)
                    && !operator.getWindowing().getTrigger().isStateful()));
    return b;
  }

  @Override
  public JavaRDD<?> translate(ReduceByKey operator, SparkExecutorContext context) {
    final JavaRDD<WindowedElement> input = (JavaRDD) context.getSingleInput(operator);

    final UnaryFunction<Iterable, Object> reducer = operator.getReducer();
    final Partitioning partitioning = operator.getPartitioning();
    final UnaryFunction<?, Long> eventTimeAssigner = operator.getEventTimeAssigner();
    final Windowing windowing =
            operator.getWindowing() == null
                    ? AttachedWindowing.INSTANCE
                    : operator.getWindowing();

    Preconditions.checkState(operator.isCombinable(),
            "Non-combinable ReduceByKey not supported!");
    Preconditions.checkState(
            !(windowing instanceof MergingWindowing),
            "MergingWindowing not supported!");
    Preconditions.checkState(!windowing.getTrigger().isStateful(),
            "Stateful triggers not supported!");

    // FIXME functions extraction could be moved to the euphoria-core
    final UnaryFunction keyExtractor;
    final UnaryFunction valueExtractor;
    if (operator.isGrouped()) {
      UnaryFunction reduceKeyExtractor = operator.getKeyExtractor();
      keyExtractor = (UnaryFunction<Pair, CompositeKey>)
              (Pair p) -> CompositeKey.of(
                      p.getFirst(),
                      reduceKeyExtractor.apply(p.getSecond()));
      UnaryFunction vfn = operator.getValueExtractor();
      valueExtractor = (UnaryFunction<Pair, Object>)
              (Pair p) -> vfn.apply(p.getSecond());
    } else {
      keyExtractor = operator.getKeyExtractor();
      valueExtractor = operator.getValueExtractor();
    }

    // ~ extract key/value + timestamp from input elements and assign windows
    JavaPairRDD<KeyedWindow, TimestampedElement> tuples = input.flatMapToPair(
            new CompositeKeyExtractor(
                    keyExtractor, valueExtractor, windowing, eventTimeAssigner));

    JavaPairRDD<KeyedWindow, TimestampedElement> reduced;
    if (partitioning.hasDefaultPartitioner()) {
      reduced = tuples.reduceByKey(new Reducer(reducer), partitioning.getNumPartitions());
    } else {
      reduced = tuples.reduceByKey(new PartitioningWrapper(partitioning), new Reducer(reducer));
    }

    return reduced.map(t -> {
      KeyedWindow kw = t._1();
      TimestampedElement el = t._2();

      // ~ extract timestamp from element rather than from KeyedWindow
      // because in KeyedWindow there is the original timestamp from
      // pre-reduce age
      long timestamp = el.getTimestamp();

      return new WindowedElement<>(kw.window(), timestamp,
              Pair.of(kw.key(), el.getElement()));
    });
  }

  /**
   * Extracts {@link KeyedWindow} from {@link WindowedElement} and
   * assigns timestamp according to (optional) eventTimeAssigner.
   */
  private static class CompositeKeyExtractor
          implements PairFlatMapFunction<WindowedElement, KeyedWindow, TimestampedElement> {

    private final UnaryFunction keyExtractor;
    private final UnaryFunction valueExtractor;
    private final Windowing windowing;
    private final UnaryFunction eventTimeAssigner;

    public CompositeKeyExtractor(UnaryFunction keyExtractor,
                                 UnaryFunction valueExtractor,
                                 Windowing windowing,
                                 UnaryFunction<?, Long> eventTimeAssigner /* optional */) {
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<KeyedWindow, TimestampedElement>> call(WindowedElement wel) throws Exception {
      if (eventTimeAssigner != null) {
        wel.setTimestamp((long) eventTimeAssigner.apply(wel.getElement()));
      }

      Set<Window> windows = windowing.assignWindowsToElement(wel);
      List<Tuple2<KeyedWindow, TimestampedElement>> out = new ArrayList<>(windows.size());
      for (Window wid : windows) {
        Object el = wel.getElement();
        long stamp = (wid instanceof TimedWindow)
                ? ((TimedWindow) wid).maxTimestamp()
                : wel.getTimestamp();
        out.add(new Tuple2<>(
                new KeyedWindow<>(wid, stamp, keyExtractor.apply(el)),
                new TimestampedElement(stamp, valueExtractor.apply(el))));
      }
      return out.iterator();
    }
  }


  private static class Reducer implements
          Function2<TimestampedElement, TimestampedElement, TimestampedElement> {

    private final UnaryFunction<Iterable, Object> reducer;

    // cached array to avoid repeated allocation
    private Object[] iterable;

    private Reducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
      this.iterable = new Object[2];
    }

    @Override
    public TimestampedElement call(TimestampedElement o1, TimestampedElement o2) {
      iterable[0] = o1.getElement();
      iterable[1] = o2.getElement();
      Object result = reducer.apply(Arrays.asList(iterable));

      return new TimestampedElement(
              Math.max(o1.getTimestamp(), o2.getTimestamp()), result);
    }
  }
}
