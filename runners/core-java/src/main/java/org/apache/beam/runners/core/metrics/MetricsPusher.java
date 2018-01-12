package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Component that regularly merges metrics and pushes them to a metrics sink. It needs to be a
 * singleton because spark runner might call initAccumulators several times and this method calls
 * {@link MetricsPusher#createAndStart} and needs to be idempotent
 */
public class MetricsPusher implements Serializable {

  public static final long DEFAULT_PERIOD = 5L;
  private static volatile MetricsPusher instance;
  private final MetricsSink metricsSink;
  private final long period;
  private MetricsContainerStepMap metricsContainerStepMap;
  private ScheduledFuture<?> scheduledFuture;
  private PipelineResult pipelineResult;

  private MetricsPusher(
      MetricsContainerStepMap metricsContainerStepMap, PipelineOptions pipelineOptions) {
    switch (pipelineOptions.getMetricsSink()) {
      case "org.apache.beam.runners.core.metrics.MetricsHttpSink":
        metricsSink = new MetricsHttpSink(pipelineOptions.getMetricsHttpSinkUrl());
        break;
      default:
        metricsSink = new DummyMetricsSink();
    }
    this.period =
        pipelineOptions.getMetricsPushPeriod() != null
            ? pipelineOptions.getMetricsPushPeriod()
            : DEFAULT_PERIOD;
    this.metricsContainerStepMap = metricsContainerStepMap;
  }

  public static void createAndStart(
      MetricsContainerStepMap metricsContainerStepMap, PipelineOptions pipelineOptions) {
    synchronized (MetricsPusher.class) {
      if (instance == null) {
        instance = new MetricsPusher(metricsContainerStepMap, pipelineOptions);
        instance.start();
      }
    }
  }

  public static MetricsPusher getInstance(){
    return instance;
  }
  /**
   * Lazily set the pipelineResult.
   * @param pipelineResult
   */
  public void setPipelineResult(PipelineResult pipelineResult){
    this.pipelineResult = pipelineResult;
  }
  private void start() {
    ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("MetricsPusher-thread")
                .build());
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
        MetricResults metricResults =
            asAttemptedOnlyMetricResults(instance.metricsContainerStepMap);
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
