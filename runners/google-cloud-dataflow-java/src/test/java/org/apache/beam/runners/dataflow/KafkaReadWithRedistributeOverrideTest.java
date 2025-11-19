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
package org.apache.beam.runners.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.io.Serializable;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaReadWithRedistributeOverrideTest implements Serializable {
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testOverrideAppliedWhenRedistributeEnabled() {
    p.apply(
        "MatchingRead",
        KafkaIO.<String, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("test_match")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withRedistribute());
    p.apply(
        "NoRedistribute",
        KafkaIO.<String, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("test_no_redistribute")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class));
    p.apply(
        "ExplicitlyDisable",
        KafkaIO.<String, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("test_disabled")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withOffsetDeduplication(false));
    p.apply(
        "ExplicitlyEnable",
        KafkaIO.<String, String>read()
            .withBootstrapServers("localhost:9092")
            .withTopic("test_enabled")
            .withKeyDeserializer(StringDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)
            .withRedistribute()
            .withOffsetDeduplication(true));

    p.replaceAll(
        Collections.singletonList(
            PTransformOverride.of(
                KafkaReadWithRedistributeOverride.matcher(),
                new KafkaReadWithRedistributeOverride.Factory<>())));

    Pipeline.PipelineVisitor visitor =
        new Pipeline.PipelineVisitor.Defaults() {

          private boolean matchingVisited = false;
          private boolean noRedistributeVisited = false;
          private boolean explicitlyDisabledVisited = false;
          private boolean explicitlyEnabledVisited = false;

          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (node.getTransform() instanceof KafkaIO.Read) {
              KafkaIO.Read<?, ?> read = (KafkaIO.Read<?, ?>) node.getTransform();
              if (read.getTopics().contains("test_match")) {
                assertThat(read.isRedistributed(), is(true));
                assertThat(read.getOffsetDeduplication(), is(true));
                matchingVisited = true;
              } else if (read.getTopics().contains("test_no_redistribute")) {
                assertThat(read.isRedistributed(), is(false));
                assertThat(read.getOffsetDeduplication(), nullValue());
                noRedistributeVisited = true;
              } else if (read.getTopics().contains("test_disabled")) {
                assertThat(read.isRedistributed(), is(false));
                assertThat(read.getOffsetDeduplication(), is(false));
                explicitlyDisabledVisited = true;
              } else if (read.getTopics().contains("test_enabled")) {
                assertThat(read.isRedistributed(), is(true));
                assertThat(read.getOffsetDeduplication(), is(true));
                explicitlyEnabledVisited = true;
              }
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void leaveCompositeTransform(Node node) {
            if (node.isRootNode()) {
              assertThat("Matching transform was not visited", matchingVisited, is(true));
              assertThat(
                  "No redistribute transform was not visited", noRedistributeVisited, is(true));
              assertThat(
                  "Explicitly disabled transform was not visited",
                  explicitlyDisabledVisited,
                  is(true));
              assertThat(
                  "Explicitly enabled transform was not visited",
                  explicitlyEnabledVisited,
                  is(true));
            }
          }
        };
    p.traverseTopologically(visitor);
  }
}
