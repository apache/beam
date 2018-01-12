package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;

/** Component that regularly merges metrics and pushes them to a metrics sink. */
public class MetricsPusher implements Serializable {

  private static MetricsPusher instance;
  private final MetricsSink metricsSink;
  private final long period;
  private MetricsContainerStepMap metricsContainerStepMap;

  private MetricsPusher(
      MetricsContainerStepMap metricsContainerStepMap, MetricsSink metricsSink, long period) {
    this.metricsContainerStepMap = metricsContainerStepMap;
    this.metricsSink = metricsSink;
    this.period = period;
    start();
  }

  public static MetricsPusher createAndStart(
      MetricsContainerStepMap metricsContainerStepMap, MetricsSink metricsSink, long period) {
    if (instance == null) {
      instance = new MetricsPusher(metricsContainerStepMap, metricsSink, period);
    }
    return instance;
  }

  private void start() {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleAtFixedRate(
        new PushingThread("MetricsPusher-thread"), 0, period, TimeUnit.SECONDS);
  }

  private class PushingThread extends Thread {

    PushingThread(String name) {
      super(name);
    }

    @Override
    public void run() {
      try {
        // merge metrics
        MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainerStepMap);
        MetricQueryResults metricQueryResults =
            metricResults.queryMetrics(MetricsFilter.builder().build());
        metricsSink.writeMetrics(metricQueryResults);
        // TODO find a condition to interrupt the pushing thread
        /*
        PipelineResult.State pipelineState = pipelineResult.getState();
        if (pipelineState.isTerminal()){
          interrupt();
        }
        */

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
