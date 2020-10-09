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
package org.apache.beam.templates;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Test of KafkaToPubsub. */
@RunWith(JUnit4.class)
public class KafkaToPubsubTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testKafkaReadingFailsWrongBootstrapServer() {
        final String bootstrapServers = "some-server:9092";
        final List<String> topicsList = new ArrayList<>(Collections.singletonList("TEST-TOPIC"));

        pipeline.apply(
                KafkaToPubsub.readFromKafka(bootstrapServers, topicsList));
        thrown.expect(Pipeline.PipelineExecutionException.class);
        thrown.expectMessage("Failed to construct kafka consumer");
        pipeline.run();
    }
}
