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

package org.apache.beam.runners.extensions.metrics;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.joda.time.Instant;
import org.junit.Test;

/**
 * Test class for MetricsHttpSink.
 */
public class MetricsHttpSinkTest {

  @Test public void testSerializer() throws Exception {
    MetricQueryResults metricQueryResults =
        new MetricQueryResults() {

          @Override
          public List<MetricResult<Long>> counters() {
            return Collections.singletonList(
                (MetricResult<Long>)
                    new MetricResult<Long>() {

                      @Override
                      public MetricName name() {
                        return new MetricName() {

                          @Override
                          public String namespace() {
                            return "ns1";
                          }

                          @Override
                          public String name() {
                            return "n1";
                          }
                        };
                      }

                      @Override
                      public String step() {
                        return "s1";
                      }

                      @Override
                      public Long committed() {
                        return 10L;
                      }

                      @Override
                      public Long attempted() {
                        return 20L;
                      }
                    });
          }

          @Override
          public List<MetricResult<DistributionResult>> distributions() {
            return Collections.singletonList(
                (MetricResult<DistributionResult>)
                    new MetricResult<DistributionResult>() {

                      @Override
                      public MetricName name() {
                        return new MetricName() {

                          @Override
                          public String namespace() {
                            return "ns1";
                          }

                          @Override
                          public String name() {
                            return "n2";
                          }
                        };
                      }

                      @Override
                      public String step() {
                        return "s2";
                      }

                      @Override
                      public DistributionResult committed() {
                        return new DistributionResult() {

                          @Override
                          public long sum() {
                            return 10L;
                          }

                          @Override
                          public long count() {
                            return 2L;
                          }

                          @Override
                          public long min() {
                            return 5L;
                          }

                          @Override
                          public long max() {
                            return 8L;
                          }
                        };
                      }

                      @Override
                      public DistributionResult attempted() {
                        return new DistributionResult() {

                          @Override
                          public long sum() {
                            return 25L;
                          }

                          @Override
                          public long count() {
                            return 4L;
                          }

                          @Override
                          public long min() {
                            return 3L;
                          }

                          @Override
                          public long max() {
                            return 9L;
                          }
                        };
                      }
                    });
          }

          @Override
          public List<MetricResult<GaugeResult>> gauges() {
            return Collections.singletonList(
                (MetricResult<GaugeResult>)
                    new MetricResult<GaugeResult>() {

                      @Override
                      public MetricName name() {
                        return new MetricName() {

                          @Override
                          public String namespace() {
                            return "ns1";
                          }

                          @Override
                          public String name() {
                            return "n3";
                          }
                        };
                      }

                      @Override
                      public String step() {
                        return "s3";
                      }

                      @Override
                      public GaugeResult committed() {
                        return new GaugeResult() {

                          @Override
                          public long value() {
                            return 100L;
                          }

                          @Override
                          public Instant timestamp() {
                            return new Instant(0L);
                          }
                        };
                      }

                      @Override
                      public GaugeResult attempted() {
                        return new GaugeResult() {

                          @Override
                          public long value() {
                            return 120L;
                          }

                          @Override
                          public Instant timestamp() {
                            return new Instant(0L);
                          }
                        };
                      }
                    });
          }
        };
    MetricsHttpSink metricsHttpSink = new MetricsHttpSink(PipelineOptionsFactory.create());
    String serializeMetrics = metricsHttpSink.serializeMetrics(metricQueryResults);
    assertEquals(
        "Errror in serialization",
        "{\"counters\":[{\"name\":\"ns1/n1\",\"step\":\"s1\",\"attempted\":20}],"
            + "\"distributions\":[{\"name\":\"ns1/n2\",\"step\":\"s2\",\"attempted\":"
            + "{\"min\":3,\"max\":9,\"sum\":25,\"count\":4,\"mean\":6.25}}],\"gauges\":"
            + "[{\"name\":\"ns1/n3\",\"step\":\"s3\",\"attempted\":{\"value\":120,\"timestamp\":"
            + "\"1970-01-01T00:00:00.000Z\"}}]}",
        serializeMetrics);
  }
}

