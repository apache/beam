/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.metrics;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The <code>Metrics</code> is a utility class for producing various kinds of metrics for
 * reporting properties of an executing pipeline.
 *
 * <p>Metrics are created by calling one of the static methods in this class. Each metric is
 * associated with a namespace and a name. The namespace allows grouping related metrics together
 * based on the definition while also disambiguating common names based on where they are defined.
 *
 * <p>Reported metrics are implicitly scoped to the transform within the pipeline that reported
 * them. This allows reporting the same metric name in multiple places and identifying the value
 * each transform reported, as well as aggregating the metric across
 *
 * <p>It is runner-dependent whether Metrics are accessible during pipeline execution or only after
 * jobs have completed.
 *
 * <p>Example:
 * <pre>{@code
 * class SomeDoFn extends DoFn<String, String> {
 *   private Counter counter = Metrics.counter(SomeDoFn.class, "my-counter");
 *
 *   {@literal @}ProcessElement
 *   public void processElement(ProcessContext c) {
 *     counter.inc();
 *     Metrics.counter(SomeDoFn.class, "my-counter2").inc();
 *   }
 * }
 * }</pre>
 *
 * <p>See {@link MetricResults} (available from the {@code PipelineResults} interface) for an
 * example off how to query metrics.
 */
@Experimental(Kind.METRICS)
public class Metrics {

  private Metrics() {}

  /**
   * Create a metric that can be incremented and decremented, and is aggregated by taking the sum.
   */
  public static Counter counter(String namespace, String name) {
    return new DelegatingCounter(MetricName.named(namespace, name));
  }

  /**
   * Create a metric that can be incremented and decremented, and is aggregated by taking the sum.
   */
  public static Counter counter(Class<?> namespace, String name) {
    return new DelegatingCounter(MetricName.named(namespace, name));
  }

  /**
   * Create a metric that records various statistics about the distribution of reported values.
   */
  public static Distribution distribution(String namespace, String name) {
    return new DelegatingDistribution(MetricName.named(namespace, name));
  }

  /**
   * Create a metric that records various statistics about the distribution of reported values.
   */
  public static Distribution distribution(Class<?> namespace, String name) {
    return new DelegatingDistribution(MetricName.named(namespace, name));
  }

  /**
   * Create a metric that can have its new value set, and is aggregated by taking the last reported
   * value.
   */
  public static Gauge gauge(String namespace, String name) {
    return new DelegatingGauge(MetricName.named(namespace, name));
  }

  /**
   * Create a metric that can have its new value set, and is aggregated by taking the last reported
   * value.
   */
  public static Gauge gauge(Class<?> namespace, String name) {
    return new DelegatingGauge(MetricName.named(namespace, name));
  }

  /** Implementation of {@link Counter} that delegates to the instance for the current context. */
  private static class DelegatingCounter implements Counter, Serializable {
    private final MetricName name;

    private DelegatingCounter(MetricName name) {
      this.name = name;
    }

    /** Increment the counter. */
    @Override public void inc() {
      inc(1);
    }

    /** Increment the counter by the given amount. */
    @Override public void inc(long n) {
      MetricsContainer container = MetricsEnvironment.getCurrentContainer();
      if (container != null) {
        container.getCounter(name).inc(n);
      }
    }

    /* Decrement the counter. */
    @Override public void dec() {
      inc(-1);
    }

    /* Decrement the counter by the given amount. */
    @Override public void dec(long n) {
      inc(-1 * n);
    }

    @Override public MetricName getName() {
      return name;
    }
  }

  /**
   * Implementation of {@link Distribution} that delegates to the instance for the current context.
   */
  private static class DelegatingDistribution implements Distribution, Serializable {
    private final MetricName name;

    private DelegatingDistribution(MetricName name) {
      this.name = name;
    }

    @Override
    public void update(long value) {
      MetricsContainer container = MetricsEnvironment.getCurrentContainer();
      if (container != null) {
        container.getDistribution(name).update(value);
      }
    }

    @Override public MetricName getName() {
      return name;
    }
  }

  /**
   * Implementation of {@link Gauge} that delegates to the instance for the current context.
   */
  private static class DelegatingGauge implements Gauge, Serializable {
    private final MetricName name;

    private DelegatingGauge(MetricName name) {
      this.name = name;
    }

    @Override
    public void set(long value) {
      MetricsContainer container = MetricsEnvironment.getCurrentContainer();
      if (container != null) {
        container.getGauge(name).set(value);
      }
    }

    @Override public MetricName getName() {
      return name;
    }
  }
}
