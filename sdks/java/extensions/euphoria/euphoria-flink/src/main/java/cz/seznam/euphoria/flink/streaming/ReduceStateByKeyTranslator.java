package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.dataset.HashPartitioner;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.functions.PartitionerWrapper;
import cz.seznam.euphoria.flink.streaming.windowing.AttachedWindow;
import cz.seznam.euphoria.flink.streaming.windowing.EmissionWindow;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Iterator;

class ReduceStateByKeyTranslator implements StreamingOperatorTranslator<ReduceStateByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public DataStream<?> translate(FlinkOperator<ReduceStateByKey> operator,
                                 StreamingExecutorContext context)
  {
    DataStream<?> input =
            Iterables.getOnlyElement(context.getInputStreams(operator));

    ReduceStateByKey origOperator = operator.getOriginalOperator();

    UnaryFunction<Collector<?>, State> stateFactory = origOperator.getStateFactory();

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

    DataStream<StreamingWindowedElement<?, ?, WindowedPair>> folded;
    // apply windowing first
    if (windowing == null) {
      WindowedStream windowedPairs =
          context.attachedWindowStream((DataStream) input, keyExtractor, valueExtractor);
      // equivalent operation to "left fold"
      folded = windowedPairs.apply(new AttachedWindowFolder(stateFactory))
          .name(operator.getName())
          .setParallelism(operator.getParallelism());
    } else {
      // FIXME merging windows not covered yet
      WindowedStream<StreamingWindowedElement<?, ?, WindowedPair>, Object, EmissionWindow<Window>>
          windowedPairs = context.windowStream((DataStream) input, keyExtractor, valueExtractor, windowing);
      // equivalent operation to "left fold"
      folded = windowedPairs.apply(
          new NonAttachedWindowFolder(windowing.getClass(), stateFactory))
          .name(operator.getName())
          .setParallelism(operator.getParallelism());
    }

    // FIXME partitioner should be applied during "keyBy" to avoid
    // unnecessary shuffle, but there is no (known) way how to set custom
    // partitioner to "keyBy" transformation

    // apply custom partitioner if different from default HashPartitioner
    if (!(origOperator.getPartitioning().getPartitioner().getClass() == HashPartitioner.class)) {
      folded = folded.partitionCustom(
              new PartitionerWrapper<>(origOperator.getPartitioning().getPartitioner()),
              p -> p.get().getKey());
    }

    return folded;
  }

  private static abstract class WindowFolder<W extends Window>
          implements WindowFunction<StreamingWindowedElement<?, ?, WindowedPair>,
                                    StreamingWindowedElement<?, ?, WindowedPair>,
                                    Object,
                                    W> {

    private final UnaryFunction<Collector<?>, State> stateFactory;

    WindowFolder(UnaryFunction<Collector<?>, State> stateFactory) {
      this.stateFactory = stateFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void apply(Object o,
                      W window,
                      Iterable<StreamingWindowedElement<?, ?, WindowedPair>> input,
                      org.apache.flink.util.Collector<StreamingWindowedElement<?, ?, WindowedPair>> out)
    {
      Iterator<StreamingWindowedElement<?, ?, WindowedPair>> it = input.iterator();

      // read the first element to obtain window metadata and key
      StreamingWindowedElement<?, ?, WindowedPair> element = it.next();
      Object key = element.get().getKey();
      WindowID wid = windowId(element.getWindowID(), window);
      long emissionWatermark = getEmissionWatermark(window);

      State state = stateFactory.apply(
          e -> out.collect(new StreamingWindowedElement<>(
              wid,
              WindowedPair.of(
                  wid.getLabel(),
                  key,
                  e))
              // ~ forward the emission watermark
              .withEmissionWatermark(emissionWatermark)));

      // add the first element to the state
      state.add(element.get().getValue());

      while (it.hasNext()) {
        state.add(it.next().get().getValue());
      }
      state.flush();
      state.close();
    }

    protected WindowID windowId(WindowID original, W window) {
      return original;
    }

    protected abstract long getEmissionWatermark(W window);
  }

  private static class NonAttachedWindowFolder
      extends WindowFolder<EmissionWindow<Window>>
  {
    private final Class<? extends Windowing> windowingType;

    NonAttachedWindowFolder(Class<? extends Windowing> windowingType,
                            UnaryFunction<Collector<?>, State> stateFactory) {
      super(stateFactory);
      this.windowingType = windowingType;
    }

    protected WindowID windowId(WindowID original, EmissionWindow<Window> window) {
      // FIXME *cough* *cough* see StreamWindower#windowIdFromSlidingFlinkWindow
      WindowID patched =
          StreamWindower.windowIdFromSlidingFlinkWindow(windowingType, window.getInner());
      if (patched != null) {
        return patched;
      }
      return original;
    }

    @Override
    protected long getEmissionWatermark(EmissionWindow<Window> window) {
      return window.getEmissionWatermark();
    }
  }

  private static class AttachedWindowFolder
      extends WindowFolder<AttachedWindow>
  {
    AttachedWindowFolder(UnaryFunction<Collector<?>, State> stateFactory) {
      super(stateFactory);
    }
    @Override
    protected long getEmissionWatermark(AttachedWindow window) {
      return window.getEmissionWatermark();
    }
  }
}
