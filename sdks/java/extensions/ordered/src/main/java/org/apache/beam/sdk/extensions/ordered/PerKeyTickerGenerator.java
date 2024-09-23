package org.apache.beam.sdk.extensions.ordered;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PTransform to generate per key tickers with certain frequency.
 *
 * @param <EventKeyT>
 * @param <EventT>
 */
class PerKeyTickerGenerator<EventKeyT, EventT>
    extends PTransform<PCollection<KV<EventKeyT, KV<Long, EventT>>>,
    PCollection<KV<EventKeyT, KV<Long, EventT>>>> {

  private static final Logger LOG = LoggerFactory.getLogger(PerKeyTickerGenerator.class);

  private final Coder<EventKeyT> eventKeyCoder;
  private final Coder<EventT> eventCoder;
  private final Duration tickerFrequency;

  PerKeyTickerGenerator(Coder<EventKeyT> eventKeyCoder, Coder<EventT> eventCoder,
      Duration tickerFrequency) {
    this.eventKeyCoder = eventKeyCoder;
    this.eventCoder = eventCoder;
    this.tickerFrequency = tickerFrequency;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized PCollection<KV<EventKeyT, KV<Long, EventT>>> expand(
      PCollection<KV<EventKeyT, KV<Long, EventT>>> input) {
    return input.apply("Generate Tickers",
            ParDo.of(new PerKeyTickerGeneratorDoFn<>(eventKeyCoder, tickerFrequency)))
        .setCoder(
            KvCoder.of(eventKeyCoder, KvCoder.of(VarLongCoder.of(), NullableCoder.of(eventCoder))));
  }

  static class PerKeyTickerGeneratorDoFn<EventKeyT, EventT>
      extends DoFn<KV<EventKeyT, KV<Long, EventT>>, KV<EventKeyT, KV<Long, EventT>>> {

    private final static String STATE = "state";
    private final static String TIMER = "timer";

    @StateId(STATE)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<EventKeyT>> stateSpec;

    @TimerId(TIMER)
    @SuppressWarnings("unused")
    private final TimerSpec tickerTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private final Duration tickerFrequency;


    PerKeyTickerGeneratorDoFn(Coder<EventKeyT> keyCoder, Duration tickerFrequency) {
      stateSpec = StateSpecs.value(keyCoder);
      this.tickerFrequency = tickerFrequency;
    }

    @ProcessElement
    public void process(
        @Element KV<EventKeyT, KV<Long, EventT>> element,
        @AlwaysFetched
        @StateId(STATE) ValueState<EventKeyT> state,
        @TimerId(TIMER) Timer tickerTimer) {
      @Nullable EventKeyT keyValue = state.read();
      if (keyValue != null) {
        return;
      }

      tickerTimer.offset(tickerFrequency).setRelative();

      state.write(element.getKey());
    }

    @OnTimer(TIMER)
    public void onTimer(
        @StateId(STATE) ValueState<EventKeyT> state,
        @TimerId(TIMER) Timer tickerTimer,
        OutputReceiver<KV<EventKeyT, KV<Long, EventT>>> outputReceiver) {

      @Nullable EventKeyT key = state.read();
      if (key == null) {
        LOG.error("Expected to get the key from the state, but got null");
        return;
      }

      // Null value will be an indicator to the main transform that the element is a ticker
      outputReceiver.output(KV.of(key, KV.of(0L, null)));
      tickerTimer.offset(tickerFrequency).setRelative();
    }
  }
}
