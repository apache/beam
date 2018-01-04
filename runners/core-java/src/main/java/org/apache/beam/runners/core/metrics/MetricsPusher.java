package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

/**
 * Component that regularly merges metrics and pushes them to a metrics sink. It needs to be a
 * singleton because spark runner might call initAccumulators several times and this method calls
 * {@link MetricsPusher#createAndStart} and needs to be idempotent
 */
public class MetricsPusher implements Serializable {

  private static volatile MetricsPusher instance;
  private final MetricsSink metricsSink;
  private final long period;
  private MetricsContainerStepMap metricsContainerStepMap;
  private ScheduledFuture<?> scheduledFuture;
  private PipelineResult pipelineResult;

  private MetricsPusher(
      MetricsContainerStepMap metricsContainerStepMap, MetricsSink metricsSink, long period) {
    this.metricsContainerStepMap = metricsContainerStepMap;
    this.metricsSink = metricsSink;
    this.period = period;

  }

  public static void createAndStart(
      MetricsContainerStepMap metricsContainerStepMap, MetricsSink metricsSink, long period) {
    synchronized (MetricsPusher.class) {
      if (instance == null) {
        instance = new MetricsPusher(metricsContainerStepMap, metricsSink, period);
        instance.start();
      }
    }
  }

  public static MetricsPusher getInstance(){
    return instance;
  }
  /**
   * Lazily set the pipelineResult
   * @param pipelineResult
   */
  public void setPipelineResult(PipelineResult pipelineResult){
    this.pipelineResult = pipelineResult;
  }
  private void start() {
    ScheduledExecutorService scheduler = Executors
        .newSingleThreadScheduledExecutor(new BasicThreadFactory.Builder().namingPattern("MetricsPusher-thread").build());
    scheduledFuture = scheduler
        .scheduleAtFixedRate(new PushingThread(), 0, period,
            TimeUnit.SECONDS);
  }
  private static void stop(){
    instance.scheduledFuture.cancel(true);
  }

  private static class PushingThread implements Runnable {

    @Override
    public void run() {
      try {
        // merge metrics
        MetricResults metricResults = asAttemptedOnlyMetricResults(instance.metricsContainerStepMap);
        MetricQueryResults metricQueryResults =
            metricResults.queryMetrics(MetricsFilter.builder().build());
        if ((Iterables.size(metricQueryResults.distributions()) != 0)
            || (Iterables.size(metricQueryResults.gauges()) != 0)
            || (Iterables.size(metricQueryResults.counters()) != 0)) {
          instance.metricsSink.writeMetrics(metricQueryResults);
        }
        if (instance.pipelineResult != null) {
          PipelineResult.State pipelineState = instance.pipelineResult.getState();
          if (pipelineState.isTerminal()) {
            stop();
          }
        }

      } catch (Exception e) {
        MetricsPushException metricsPushException = new MetricsPushException(e);
        metricsPushException.printStackTrace();
      }
    }
  }
  private static class MetricsPushException extends Exception{

    MetricsPushException(Throwable cause) {
      super(cause);
    }
  }
}
