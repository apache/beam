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
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.EmissionWindow;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Arrays;

class ReduceByKeyTranslator implements StreamingOperatorTranslator<ReduceByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceByKey> operator,
                                 StreamingExecutorContext context)
  {
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

    // FIXME non-combinable reduce function not supported without windowing
    if (!origOperator.isCombinable()) {
      throw new UnsupportedOperationException("Non-combining reduce not supported yet");
    }

    DataStream<StreamingWindowedElement<?, ?, WindowedPair>> reduced;
    if (windowing == null) {
      WindowedStream windowedPairs = context.attachedWindowStream(
          (DataStream) input, keyExtractor, valueExtractor);
      reduced = windowedPairs.reduce(new TypedReducer(reducer))
          .name(operator.getName())
          .setParallelism(operator.getParallelism());
    } else {
      // apply windowing first
      WindowedStream<StreamingWindowedElement<?, ?, WindowedPair>, Object, EmissionWindow<Window>>
          windowedPairs = context.windowStream((DataStream) input, keyExtractor, valueExtractor, windowing);
      // reduce
      reduced = windowedPairs
          .apply(new TypedReducer(reducer), new ForwardEmissionWatermark())
          .name(operator.getName())
          .setParallelism(operator.getParallelism());
    }

    // FIXME partitioner should be applied during "reduce" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default HashPartitioner
    if (!(origOperator.getPartitioning().getPartitioner().getClass() == HashPartitioner.class)) {
      reduced = reduced.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.get().getKey());
    }

    return reduced;
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

    @Override
    public void apply(Object o,
                      EmissionWindow<Window> window,
                      Iterable<StreamingWindowedElement<?, ?, WindowedPair>> input,
                      Collector<StreamingWindowedElement<?, ?, WindowedPair>> out)
        throws Exception
    {
      for (StreamingWindowedElement<?, ?, WindowedPair> i : input) {
        out.collect(i.withEmissionWatermark(window.getEmissionWatermark()));
      }
    }
  } // ~ end of ForwardEmissionWatermark

  private static class TypedReducer
          implements ReduceFunction<StreamingWindowedElement<?, ?, WindowedPair>>,
          ResultTypeQueryable<StreamingWindowedElement<?, ?, WindowedPair>>
  {
    final UnaryFunction<Iterable, Object> reducer;

    public TypedReducer(UnaryFunction<Iterable, Object> reducer) {
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
  } // ~ end of TypedReducer
}
