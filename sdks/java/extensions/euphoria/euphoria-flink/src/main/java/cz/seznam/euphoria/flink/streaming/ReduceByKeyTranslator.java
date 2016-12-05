package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.IteratorIterable;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.FlinkWindow;
import cz.seznam.euphoria.flink.streaming.windowing.MultiWindowedElement;
import cz.seznam.euphoria.flink.streaming.windowing.MultiWindowedElementWindowFunction;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterators;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

class ReduceByKeyTranslator implements StreamingOperatorTranslator<ReduceByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceByKey> operator,
                                 StreamingExecutorContext context) {
    DataStream<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceByKey origOperator = operator.getOriginalOperator();
    final UnaryFunction<Iterable, Object> reducer = origOperator.getReducer();
    final UnaryFunction keyExtractor = origOperator.getKeyExtractor();
    final UnaryFunction valueExtractor = origOperator.getValueExtractor();
    final Windowing windowing = origOperator.getWindowing();
    final UnaryFunction eventTimeAssigner = origOperator.getEventTimeAssigner();

    // apply windowing first
    SingleOutputStreamOperator<StreamingWindowedElement<?, Pair>> reduced;
    if (windowing == null) {
      WindowedStream windowed =
          context.attachedWindowStream((DataStream) input, keyExtractor, valueExtractor);
      if (origOperator.isCombinable()) {
        // reduce incrementally
        reduced = windowed.apply(
            new StreamingWindowedElementIncrementalReducer(reducer), new PassThroughWindowFunction<>());
      } else {
        // reduce all elements at once when the window is fired
        reduced = windowed.apply(
            new StreamingWindowedElementWindowedReducer(reducer, new PassThroughWindowFunction<>()));
      }
    } else {
      WindowedStream windowed = context.flinkWindow(
              (DataStream) input, keyExtractor, valueExtractor, windowing, eventTimeAssigner);
      if (origOperator.isCombinable()) {
        // reduce incrementally
        reduced = windowed.apply(
            new MultiWindowedElementIncrementalReducer(reducer), new MultiWindowedElementWindowFunction());

      } else {
        // reduce all elements at once when the window is fired
        reduced = windowed.apply(
            new MultiWindowedElementWindowedReducer(reducer, new MultiWindowedElementWindowFunction()));
      }
    }

    DataStream<StreamingWindowedElement<?, Pair>> out =
            reduced.name(operator.getName())
                   .setParallelism(operator.getParallelism());

    // FIXME partitioner should be applied during "reduce" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default
    if (!origOperator.getPartitioning().hasDefaultPartitioner()) {
      out = out.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.getElement().getKey());
    }

    return out;
  }

  /**
   * Performs incremental reduction (in case of combining reduce).
   */
  private static class StreamingWindowedElementIncrementalReducer
          implements ReduceFunction<StreamingWindowedElement<?, Pair>>,
          ResultTypeQueryable<StreamingWindowedElement<?, Pair>> {

    final UnaryFunction<Iterable, Object> reducer;

    public StreamingWindowedElementIncrementalReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public StreamingWindowedElement<?, Pair> reduce(
            StreamingWindowedElement<?, Pair> p1,
            StreamingWindowedElement<?, Pair> p2) {

      Object v1 = p1.getElement().getSecond();
      Object v2 = p2.getElement().getSecond();
      return new StreamingWindowedElement<>(
        p1.getWindow(),
        p1.getTimestamp(),
        Pair.of(p1.getElement().getKey(), reducer.apply(Arrays.asList(v1, v2))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<StreamingWindowedElement<?, Pair>> getProducedType() {
      return TypeInformation.of((Class) StreamingWindowedElement.class);
    }
  } // ~ end of StreamingWindowedElementIncrementalReducer

  /**
   * Performs non-incremental reduction (in case of non-combining reduce).
   */
  private static class StreamingWindowedElementWindowedReducer
          implements WindowFunction<
          StreamingWindowedElement<?, Pair>,
          StreamingWindowedElement<?, Pair>,
          Object,
          Window> {

    private final UnaryFunction<Iterable, Object> reducer;
    private final WindowFunction<StreamingWindowedElement<?, Pair>,
            StreamingWindowedElement<?, Pair>, Object, Window> emissionFunction;

    @SuppressWarnings("unchecked")
    public StreamingWindowedElementWindowedReducer(UnaryFunction<Iterable, Object> reducer,
                                                   WindowFunction emissionFunction) {
      this.reducer = reducer;
      this.emissionFunction = emissionFunction;
    }

    @Override
    public void apply(Object key,
                      Window window,
                      Iterable<StreamingWindowedElement<?, Pair>> input,
                      Collector<StreamingWindowedElement<?, Pair>> collector)
            throws Exception {

      Iterator<StreamingWindowedElement<?, Pair>> it = input.iterator();

      // read the first element to obtain window metadata
      StreamingWindowedElement<?, Pair> element = it.next();
      cz.seznam.euphoria.core.client.dataset.windowing.Window wid = element.getWindow();
      long emissionWatermark = element.getTimestamp();

      // concat the already read element with rest of the opened iterator
      Iterator<StreamingWindowedElement<?, Pair>> concatIt =
              Iterators.concat(Iterators.singletonIterator(element), it);

      // unwrap all elements to be used in user defined reducer
      Iterator<Object> unwrapped =
              Iterators.transform(concatIt, e -> e.getElement().getValue());

      Object reduced = reducer.apply(new IteratorIterable<>(unwrapped));

      StreamingWindowedElement<?, Pair> out =
          new StreamingWindowedElement<>(wid, emissionWatermark, Pair.of(key, reduced));

      // decorate resulting item with emission watermark from fired window
      emissionFunction.apply(key, window, Collections.singletonList(out), collector);
    }
  } // ~ end of StreamingWindowedElementWindowedReducer


  /**
   * Performs incremental reduction (in case of combining reduce) on
   * {@link MultiWindowedElement}s. Assumes the result is emitted using
   * {@link MultiWindowedElementWindowFunction}.
   */
  private static class MultiWindowedElementIncrementalReducer<
          WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window, KEY, VALUE>
      implements ReduceFunction<MultiWindowedElement<WID, Pair<KEY, VALUE>>>,
      ResultTypeQueryable<MultiWindowedElement<WID, Pair<KEY, VALUE>>> {

    final UnaryFunction<Iterable<VALUE>, VALUE> reducer;

    public MultiWindowedElementIncrementalReducer(UnaryFunction<Iterable<VALUE>, VALUE> reducer) {
      this.reducer = reducer;
    }

    @Override
    public MultiWindowedElement<WID, Pair<KEY, VALUE>> reduce(
        MultiWindowedElement<WID, Pair<KEY, VALUE>> p1,
        MultiWindowedElement<WID, Pair<KEY, VALUE>> p2) {

      VALUE v1 = p1.getElement().getSecond();
      VALUE v2 = p2.getElement().getSecond();
      Set<WID> s = Collections.emptySet();
      return new MultiWindowedElement<>(s,
          Pair.of(p1.getElement().getFirst(), reducer.apply(Arrays.asList(v1, v2))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<MultiWindowedElement<WID, Pair<KEY, VALUE>>>
    getProducedType() {
      return TypeInformation.of((Class) MultiWindowedElement.class);
    }
  } // ~ end of MultiWindowedElementIncrementalReducer

  /**
   * Performs non-incremental reduction (in case of non-combining reduce).
   */
  private static class MultiWindowedElementWindowedReducer<
          WID extends cz.seznam.euphoria.core.client.dataset.windowing.Window,
          KEY, VALUEIN, VALUEOUT>
      implements WindowFunction<
      MultiWindowedElement<?, Pair<KEY, VALUEIN>>,
      StreamingWindowedElement<WID, Pair<KEY, VALUEOUT>>,
      KEY,
      FlinkWindow<WID>> {

    private final UnaryFunction<Iterable<VALUEIN>, VALUEOUT> reducer;
    private final MultiWindowedElementWindowFunction<WID, KEY, VALUEOUT> emissionFunction;

    public MultiWindowedElementWindowedReducer(
        UnaryFunction<Iterable<VALUEIN>, VALUEOUT> reducer,
        MultiWindowedElementWindowFunction<WID, KEY, VALUEOUT> emissionFunction) {
      this.reducer = reducer;
      this.emissionFunction = emissionFunction;
    }

    @Override
    public void apply(KEY key,
                      FlinkWindow<WID> window,
                      Iterable<MultiWindowedElement<?, Pair<KEY, VALUEIN>>> input,
                      Collector<StreamingWindowedElement<WID, Pair<KEY, VALUEOUT>>> collector)
        throws Exception {

      VALUEOUT reducedValue = reducer.apply(new IteratorIterable<>(
          Iterators.transform(input.iterator(), e -> e.getElement().getValue())));
      MultiWindowedElement<WID, Pair<KEY, VALUEOUT>> reduced =
          new MultiWindowedElement<>(Collections.emptySet(), Pair.of(key, reducedValue));
      this.emissionFunction.apply(key, window,
          Collections.singletonList(reduced), collector);
    }
  } // ~ end of MultiWindowedElementWindowedReducer

}
