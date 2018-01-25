package org.apache.beam.runners.core.metrics;

import static org.apache.beam.runners.core.metrics.MetricsContainerStepMap.asAttemptedOnlyMetricResults;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
 * {@link MetricsPusher#init} and needs to be idempotent
 */
public class MetricsPusher implements Serializable {

  public static final long DEFAULT_PERIOD = 5L;
  private static volatile MetricsPusher instance;
  private static MetricsSink metricsSink;
  private static long period;
  private static List<MetricsContainerStepMap> metricsContainerStepMaps;
  private static ScheduledFuture<?> scheduledFuture;
  private static PipelineResult pipelineResult;

  private MetricsPusher(PipelineOptions pipelineOptions) {
    switch (pipelineOptions.getMetricsSink()) {
      case "org.apache.beam.runners.core.metrics.MetricsHttpSink":
        metricsSink = new MetricsHttpSink(pipelineOptions.getMetricsHttpSinkUrl());
        break;
      default:
        metricsSink = new DummyMetricsSink();
    }
    period =
        pipelineOptions.getMetricsPushPeriod() != null
            ? pipelineOptions.getMetricsPushPeriod()
            : DEFAULT_PERIOD;
    metricsContainerStepMaps = new ArrayList<>();
  }

  private static void addMetricsContainerStepMap(MetricsContainerStepMap metricsContainerStepMap){
    if (metricsContainerStepMaps.indexOf(metricsContainerStepMap) == -1) {
      metricsContainerStepMaps.add(metricsContainerStepMap);
    }
  }

  public static synchronized void init(
      MetricsContainerStepMap metricsContainerStepMap, PipelineOptions pipelineOptions) {
    if (instance == null) {
      instance = new MetricsPusher(pipelineOptions);
      addMetricsContainerStepMap(metricsContainerStepMap);
      start();
    } else {
      /*
      MetricsPusher.init will be called several times in a pipeline
      (e.g Flink calls it with each UDF with a different MetricsContainerStepMap instance).
      Then the singleton needs to register a new metricsContainerStepMap to merge
      */
      addMetricsContainerStepMap(metricsContainerStepMap);
      /*    if the thread is stopped, then it means that the pipeline is finished.
      If we run multiple pipelines in the same JVM, as MetricsPusher is a singleton,
      the instance will still be in memory but the thread will be stopped.*/
      if (scheduledFuture.isCancelled()) {
        start();
      }
    }
  }

  /**
   * Lazily set the pipelineResult.
   * @param pr
   */
  public static void setPipelineResult(PipelineResult pr){
    pipelineResult = pr;
  }
  private static void start() {
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
  private static synchronized void stop(){
    if (!scheduledFuture.isCancelled()) {
      scheduledFuture.cancel(true);
    }
  }

  public static void pushMetrics(){
    try {
      // merge metrics
      for (MetricsContainerStepMap metricsContainerStepMap : metricsContainerStepMaps) {
        MetricResults metricResults = asAttemptedOnlyMetricResults(metricsContainerStepMap);
        MetricQueryResults metricQueryResults =
            metricResults.queryMetrics(MetricsFilter.builder().build());
        if ((Iterables.size(metricQueryResults.distributions()) != 0)
            || (Iterables.size(metricQueryResults.gauges()) != 0)
            || (Iterables.size(metricQueryResults.counters()) != 0)) {
          metricsSink.writeMetrics(metricQueryResults);
        }
      }
      if (pipelineResult != null) {
        PipelineResult.State pipelineState = pipelineResult.getState();
        if (pipelineState.isTerminal()) {
          stop();
        }
      }

    } catch (Exception e) {
      MetricsPushException metricsPushException = new MetricsPushException(e);
      metricsPushException.printStackTrace();
    }
  }

  private static class PushingThread implements Runnable {

    @Override
    public void run() {
      pushMetrics();
    }
  }

  private static class MetricsPushException extends Exception{
    MetricsPushException(Throwable cause) {
      super(cause);
    }
  }
}
