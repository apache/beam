package org.apache.beam.fn.harness;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerMetricsTestDoFn extends DoFn<KV<String, Long>, KV<String, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(TimerMetricsTestDoFn.class);

    public static final String TIMER_ID = "myTestTimer";

    @TimerId(TIMER_ID)
    private final TimerSpec timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    private final Counter timersFiredCounter =
            Metrics.counter(TimerMetricsTestDoFn.class, "timersFired");

    @ProcessElement
    public void processElement(
            ProcessContext c,
            @TimerId(TIMER_ID) Timer timer) {
        LOG.info("Processing element: {}", c.element());
        // Set a timer to fire very quickly for processing time.
        timer.offset(Duration.millis(1)).setRelative();
        LOG.info("Set timer for element: {}", c.element());
        c.output(c.element());
    }

    @OnTimer(TIMER_ID)
    public void onTimerCallback(
            OnTimerContext c) {
        LOG.info("Timer fired for key: {}, window: {}, timestamp: {}", c.key(), c.window(), c.timestamp());
        timersFiredCounter.inc();
    }
}
