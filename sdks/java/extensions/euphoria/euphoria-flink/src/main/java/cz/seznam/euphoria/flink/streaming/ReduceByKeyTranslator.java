package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

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

    // apply windowing first
    WindowedStream<WindowedElement<?, ?, WindowedPair>, Object, Window> windowedPairs =
            context.windowStream((DataStream) input, keyExtractor, valueExtractor, windowing);

    // FIXME non-combinable reduce function not supported without windowing
    if (!origOperator.isCombinable()) {
      throw new UnsupportedOperationException("Non-combining reduce not supported yet");
    }

    // reduce
    DataStream<WindowedElement<?, ?, WindowedPair>> reduced =
        windowedPairs.reduce(new TypedReducer(reducer))
            .name(operator.getName())
            .setParallelism(operator.getParallelism());

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

  private static class TypedReducer
          implements ReduceFunction<WindowedElement<?, ?, WindowedPair>>,
          ResultTypeQueryable<WindowedElement<?, ?, WindowedPair>>
  {
    final UnaryFunction<Iterable, Object> reducer;

    public TypedReducer(UnaryFunction<Iterable, Object> reducer) {
      this.reducer = reducer;
    }

    @Override
    public WindowedElement<?, ?, WindowedPair> reduce(
        WindowedElement<?, ?, WindowedPair> p1,
        WindowedElement<?, ?, WindowedPair> p2) {

      Object v1 = p1.get().getSecond();
      Object v2 = p2.get().getSecond();
      WindowID<?, ?> wid = p1.getWindowID();
      return new WindowedElement<>(
          wid,
          WindowedPair.of(
              wid.getLabel(),
              p1.get().getKey(),
              reducer.apply(Arrays.asList(v1, v2))));
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<WindowedElement<?, ?, WindowedPair>> getProducedType() {
      return TypeInformation.of((Class) WindowedElement.class);
    }
  }
}
