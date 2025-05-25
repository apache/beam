package org.apache.beam.fn.harness;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class FnApiDoFnRunnerTimerMetricsTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testMetricsInTimerCallback() {
        List<KV<String, Long>> inputElements = new ArrayList<>();
        inputElements.add(KV.of("key1", 1L));
        inputElements.add(KV.of("key2", 2L));
        inputElements.add(KV.of("key3", 3L));

        pipeline.apply(Create.of(inputElements)
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
                .apply(ParDo.of(new TimerMetricsTestDoFn()));

        PipelineResult result = pipeline.run();
        result.waitUntilFinish(); // Ensure pipeline processing is complete

        MetricQueryResults metrics = result.metrics().queryMetrics(
                MetricsFilter.builder()
                        .addNameFilter(MetricNameFilter.named(TimerMetricsTestDoFn.class, "timersFired"))
                        .build());

        long timersFiredCount = 0;
        for (MetricResult<Long> counter : metrics.getCounters()) {
            if (counter.getName().getName().equals("timersFired")) {
                // In tests, attempted value is usually what we want for counters.
                timersFiredCount = counter.getAttempted(); 
                break;
            }
        }
        assertEquals("Counter 'timersFired' should reflect all timers that fired.", inputElements.size(), timersFiredCount);
    }
}
