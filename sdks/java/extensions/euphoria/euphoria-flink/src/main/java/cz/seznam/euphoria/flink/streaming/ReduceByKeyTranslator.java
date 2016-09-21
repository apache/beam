package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.IteratorIterable;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.EmissionWindow;
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

class ReduceByKeyTranslator implements StreamingOperatorTranslator<ReduceByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceByKey> operator,
                                 StreamingExecutorContext context) {
    DataStream<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceByKey origOperator = operator.getOriginalOperator();
    final UnaryFunction<Iterable, Object> reducer = origOperator.getReducer();
    final UnaryFunction keyExtractor;
    final UnaryFunction valueExtractor;
    final Windowing windowing = origOperator.getWindowing();

    if (origOperator.isGrouped()) {
      UnaryFunction reduceKeyExtractor = origOperator.getKeyExtractor();
      keyExtractor = (UnaryFunction<Pair, CompositeKey>)
              (Pair p) -> CompositeKey.of(
                      p.getFirst(),
                      reduceKeyExtractor.apply(p.getSecond()));
      UnaryFunction vfn = origOperator.getValueExtractor();
      valueExtractor = (UnaryFunction<Pair, Object>)
              (Pair p) -> vfn.apply(p.getSecond());
    } else {
      keyExtractor = origOperator.getKeyExtractor();
      valueExtractor = origOperator.getValueExtractor();
    }

    // apply windowing first
    WindowedStream windowedPairs;
    WindowFunction<?, ?, ?, ?> emissionFunction;
    if (windowing == null) {
      windowedPairs = context.attachedWindowStream(
              (DataStream) input, keyExtractor, valueExtractor);
      emissionFunction = new PassThroughWindowFunction<>();
    } else {
      windowedPairs = context.windowStream(
              (DataStream) input, keyExtractor, valueExtractor, windowing);
      emissionFunction = new ForwardEmissionWatermark(windowing.getClass());
    }

    SingleOutputStreamOperator<StreamingWindowedElement<?, ?, WindowedPair>> reduced;
    // ~ ForwardEmissionWatermark will
    // 1) assign the {@link StreamingWindowedElement#emissionWatermark}
    // 2) re-assign the {@link StreamingWindowedElement#windowId} which fixes
    // the short comings of {@link StreamWindower#window} for time sliding windows
    if (origOperator.isCombinable()) {
      // reduce incrementally
      reduced = windowedPairs.apply(
              new IncrementalReducer(reducer), emissionFunction);
    } else {
      // reduce all elements at once when the window is fired
      reduced = windowedPairs.apply(
              new WindowedReducer(reducer, emissionFunction));
    }

    DataStream<StreamingWindowedElement<?, ?, WindowedPair>> out =
            reduced.name(operator.getName())
                   .setParallelism(operator.getParallelism());

    // FIXME partitioner should be applied during "reduce" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default HashPartitioner
    if (!(origOperator.getPartitioning().getPartitioner().getClass() == HashPartitioner.class)) {
      out = out.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.get().getKey());
    }

    return out;
  }

  /**
   * Windowing function to extract the emission watermark from the window being
   * emitted and forward it along the emitted element(s).
   */
  private static class ForwardEmissionWatermark
      implements WindowFunction<
      StreamingWindowedElement<?, ?, WindowedPair>,
      StreamingWindowedElement<?, ?, WindowedPair>,
      Object,
      EmissionWindow<Window>> {

    private final Class<? extends Windowing> windowingType;

    public ForwardEmissionWatermark(Class<? extends Windowing> windowingType) {
      this.windowingType = windowingType;
    }

    @Override
    public void apply(Object o,
                      EmissionWindow<Window> window,
                      Iterable<StreamingWindowedElement<?, ?, WindowedPair>> input,
                      Collector<StreamingWindowedElement<?, ?, WindowedPair>> out) {
      for (StreamingWindowedElement<?, ?, WindowedPair> i : input) {
        // FIXME *cough* *cough* see StreamWindower#windowIdFromSlidingFlinkWindow
        final WindowID<?, ?> wid;
        wid = StreamWindower.windowIdFromSlidingFlinkWindow(
            windowingType, window.getInner());
        if (wid != null) {
          WindowedPair wp = i.get();
          wp = WindowedPair.of(wid.getLabel(), wp.getFirst(), wp.getSecond());
          i = new StreamingWindowedElement<>(wid, wp);
        }
        out.collect(i.withEmissionWatermark(window.getEmissionWatermark()));
      }
    }
  } // ~ end of ForwardEmissionWatermark

  /**
   * Performs incremental reduction (in case of combining reduce).
   */
  private static class IncrementalReducer
          implements ReduceFunction<StreamingWindowedElement<?, ?, WindowedPair>>,
          ResultTypeQueryable<StreamingWindowedElement<?, ?, WindowedPair>> {

    final UnaryFunction<Iterable, Object> reducer;

    public IncrementalReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public StreamingWindowedElement<?, ?, WindowedPair> reduce(
        StreamingWindowedElement<?, ?, WindowedPair> p1,
        StreamingWindowedElement<?, ?, WindowedPair> p2) {

      Object v1 = p1.get().getSecond();
      Object v2 = p2.get().getSecond();
      WindowID<?, ?> wid = p1.getWindowID();
      StreamingWindowedElement<?, ?, WindowedPair>
          out = new StreamingWindowedElement<>(
          wid,
          WindowedPair.of(
              wid.getLabel(),
              p1.get().getKey(),
              reducer.apply(Arrays.asList(v1, v2))));
      // ~ forward the emission watermark - if any
      out.withEmissionWatermark(p1.getEmissionWatermark());
      return out;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<StreamingWindowedElement<?, ?, WindowedPair>> getProducedType() {
      return TypeInformation.of((Class) StreamingWindowedElement.class);
    }
  } // ~ end of IncrementalReducer

  /**
   * Performs non-incremental reduction (in case of non-combining reduce).
   */
  private static class WindowedReducer
          implements WindowFunction<
          StreamingWindowedElement<?, ?, WindowedPair>,
          StreamingWindowedElement<?, ?, WindowedPair>,
          Object,
          Window> {

    private final UnaryFunction<Iterable, Object> reducer;
    private final WindowFunction<StreamingWindowedElement<?, ?, WindowedPair>,
            StreamingWindowedElement<?, ?, WindowedPair>, Object, Window> emissionFunction;

    @SuppressWarnings("unchecked")
    public WindowedReducer(UnaryFunction<Iterable, Object> reducer,
                           WindowFunction emissionFunction) {
      this.reducer = reducer;
      this.emissionFunction = emissionFunction;
    }

    @Override
    public void apply(Object key,
                      Window window,
                      Iterable<StreamingWindowedElement<?, ?, WindowedPair>> input,
                      Collector<StreamingWindowedElement<?, ?, WindowedPair>> collector)
            throws Exception {

      Iterator<StreamingWindowedElement<?, ?, WindowedPair>> it = input.iterator();

      // read the first element to obtain window metadata
      StreamingWindowedElement<?, ?, WindowedPair> element = it.next();
      WindowID<?, ?> wid = element.getWindowID();
      long emissionWatermark = element.getEmissionWatermark();

      // concat the already read element with rest of the opened iterator
      Iterator<StreamingWindowedElement<?, ?, WindowedPair>> concatIt =
              Iterators.concat(Iterators.singletonIterator(element), it);

      // unwrap all elements to be used in user defined reducer
      Iterator<Object> unwrapped =
              Iterators.transform(concatIt, e -> e.get().getValue());

      Object reduced = reducer.apply(new IteratorIterable<>(unwrapped));

      StreamingWindowedElement<?, ?, WindowedPair>
              out = new StreamingWindowedElement<>(
              wid,
              WindowedPair.of(
                      wid.getLabel(),
                      key,
                      reduced));
      out.withEmissionWatermark(emissionWatermark);

      // decorate resulting item with emission watermark from fired window
      emissionFunction.apply(key, window, Collections.singletonList(out), collector);
    }
  } // ~ end of WindowedReducer
}
