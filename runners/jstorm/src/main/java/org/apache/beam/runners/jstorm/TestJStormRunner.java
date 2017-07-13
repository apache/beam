package org.apache.beam.runners.jstorm;

import avro.shaded.com.google.common.collect.Maps;
import com.alibaba.jstorm.common.metric.AsmMetric;
import com.alibaba.jstorm.metric.*;
import com.alibaba.jstorm.utils.JStormUtils;
import com.google.common.base.Optional;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Test JStorm runner.
 */
public class TestJStormRunner extends PipelineRunner<StormRunner.StormPipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(TestJStormRunner.class);

    public static TestJStormRunner fromOptions(PipelineOptions options) {
        return new TestJStormRunner(options.as(StormPipelineOptions.class));
    }

    private final StormRunner stormRunner;
    private final StormPipelineOptions options;

    private TestJStormRunner(StormPipelineOptions options) {
        this.options = options;
        Map conf = Maps.newHashMap();
        //conf.put(ConfigExtension.KV_STORE_TYPE, KvStoreManagerFactory.KvStoreType.memory.toString());
        options.setTopologyConfig(conf);
        options.setLocalMode(true);
        stormRunner = StormRunner.fromOptions(checkNotNull(options, "options"));
    }

    @Override
    public StormRunner.StormPipelineResult run(Pipeline pipeline) {
        StormRunner.StormPipelineResult result = stormRunner.run(pipeline);

        try {
            int numberOfAssertions = PAssert.countAsserts(pipeline);

            LOG.info("Running JStorm job {} with {} expected assertions.", result.getTopologyName(), numberOfAssertions);
            if(numberOfAssertions == 0) {
                // If assert number is zero, wait 5 sec
                JStormUtils.sleepMs(5000);
                return result;
            } else {
                for (int i = 0; i < 40; ++i) {
                    Optional<Boolean> success = checkForPAssertSuccess(numberOfAssertions);
                    if (success.isPresent() && success.get()) {
                        return result;
                    } else if (success.isPresent() && !success.get()) {
                        throw new AssertionError("Failed assertion checks.");
                    } else {
                        JStormUtils.sleepMs(500);
                    }
                }
                LOG.info("Assertion checks timed out.");
                throw new AssertionError("Assertion checks timed out.");
            }
        } finally {
            clearPAssertCount();
            cancel(result);
        }
    }

    private Optional<Boolean> checkForPAssertSuccess(int expectedNumberOfAssertions) {
        int successes = 0;
        for (AsmMetric metric : JStormMetrics.search(PAssert.SUCCESS_COUNTER, MetaType.TASK, MetricType.COUNTER)) {
            successes += ((Long) metric.getValue(AsmWindow.M1_WINDOW)).intValue();
        }
        int failures = 0;
        for (AsmMetric metric : JStormMetrics.search(PAssert.FAILURE_COUNTER, MetaType.TASK, MetricType.COUNTER)) {
            failures += ((Long) metric.getValue(AsmWindow.M1_WINDOW)).intValue();
        }

        if (failures > 0) {
            LOG.info("Found {} success, {} failures out of {} expected assertions.",
                    successes, failures, expectedNumberOfAssertions);
            return Optional.of(false);
        } else if (successes >= expectedNumberOfAssertions) {
            LOG.info("Found {} success, {} failures out of {} expected assertions.",
                    successes, failures, expectedNumberOfAssertions);
            return Optional.of(true);
        }

        LOG.info("Found {} success, {} failures out of {} expected assertions.",
                successes, failures, expectedNumberOfAssertions);
        return Optional.absent();
    }

    private void clearPAssertCount() {
        String topologyName = options.getJobName();
        AsmMetricRegistry taskMetrics = JStormMetrics.getTaskMetrics();
        Iterator<Map.Entry<String, AsmMetric>> itr = taskMetrics.getMetrics().entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, AsmMetric> metric = itr.next();
            if (metric.getKey().contains(topologyName)) {
                itr.remove();
            }
        }
    }

    private void cancel(StormRunner.StormPipelineResult result) {
        try {
            result.cancel();
        } catch (IOException e) {
            throw new RuntimeException("Failed to cancel.", e);
}
    }
}
