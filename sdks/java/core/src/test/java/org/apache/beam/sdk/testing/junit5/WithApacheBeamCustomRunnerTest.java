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
package org.apache.beam.sdk.testing.junit5;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.GaugeResult;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipelineHandler;
import org.apache.beam.sdk.transforms.Create;
import org.joda.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@WithApacheBeam(
    options = {
        "--runner=org.apache.beam.sdk.testing.junit5.WithApacheBeamCustomRunnerTest$MyTestRunner"
    }
)
// just until we can fix the project setup and surefire config
@RunWith(JUnitPlatform.class)
class WithApacheBeamCustomRunnerTest {
    @BeamInject
    private TestPipelineHandler<?> pipeline;

    @Test
    void injections(final TestPipelineHandler<?> pipeline) {
        assertNotNull(pipeline);
        assertEquals(pipeline, this.pipeline);
    }

    @Test
    void configuration(final TestPipelineHandler<?> pipeline) {
        pipeline.apply(Create.of("a", "b")); // runner is a mock so whatever is set it suceeds
        assertEquals(PipelineResult.State.DONE, pipeline.run().getState());
    }

    public static class MyTestRunner extends PipelineRunner<PipelineResult> {
        public static MyTestRunner fromOptions(final PipelineOptions options) {
            return new MyTestRunner();
        }

        @Override
        public PipelineResult run(final Pipeline pipeline) {
            return new PipelineResult() {
                @Override
                public State getState() {
                    return State.DONE;
                }

                @Override
                public State cancel() throws IOException {
                    return State.CANCELLED;
                }

                @Override
                public State waitUntilFinish(final Duration duration) {
                    return getState();
                }

                @Override
                public State waitUntilFinish() {
                    return getState();
                }

                @Override
                public MetricResults metrics() {
                    return new MetricResults() {
                        @Override
                        public MetricQueryResults queryMetrics(final MetricsFilter filter) {
                            return new MetricQueryResults() {
                                @Override
                                public Iterable<MetricResult<Long>> counters() {
                                    return emptyList();
                                }

                                @Override
                                public Iterable<MetricResult<DistributionResult>> distributions() {
                                    return emptyList();
                                }

                                @Override
                                public Iterable<MetricResult<GaugeResult>> gauges() {
                                    return emptyList();
                                }
                            };
                        }
                    };
                }
            };
        }
    }
}
