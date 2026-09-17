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
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.kafka.streams.KafkaStreamsPipelineOptions;
import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.kafka.streams.TopologyDescription;
import org.junit.Test;

/**
 * Pins down what happens to a Flatten whose branches would run at different parallelisms — one
 * through a GroupByKey and so at the shuffle's parallelism, one straight from a source and so a
 * single instance.
 *
 * <p>This matters because a Flatten runs as one set of tasks over all of its inputs. Kafka Streams
 * merges the subtopologies of every parent a processor is wired to and gives the result as many
 * tasks as its largest source topic has partitions, so a parent with fewer partitions would only
 * produce on some of those tasks and the rest would wait forever for a watermark report from it.
 *
 * <p>That does not arise, and these tests record why: the fuser folds such a Flatten into the SDK
 * harness stages rather than leaving a node for the runner to translate, so the branches never
 * share a subtopology and no Flatten node exists to run at a single parallelism. The Flattens that
 * do reach {@link FlattenTranslator} come from the fuser deduplicating partial outputs of a single
 * PCollection. If a change ever makes the mismatched shape reach the translator, these tests start
 * failing and the partition-count handling there needs revisiting.
 */
public class FlattenParallelismTest {

  /** The name given to the Flatten below, which no topology node should be derived from. */
  private static final String FLATTEN_NAME = "merge";

  private static class ToKvFn extends DoFn<Integer, KV<String, Integer>> {
    @ProcessElement
    public void processElement(@Element Integer input, OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of("k", input));
    }
  }

  private static class UngroupFn extends DoFn<KV<String, Iterable<Integer>>, Integer> {
    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<Integer>> group, OutputReceiver<Integer> out) {
      for (int value : group.getValue()) {
        out.output(value);
      }
    }
  }

  private static Pipeline mixedParallelismFlatten(int internalParallelism) {
    KafkaStreamsPipelineOptions options =
        KafkaStreamsTestRunner.testOptions().as(KafkaStreamsPipelineOptions.class);
    options.setInternalParallelism(internalParallelism);
    Pipeline pipeline = Pipeline.create(options);

    // Through a GroupByKey, so this branch runs at the shuffle's parallelism.
    PCollection<Integer> shuffled =
        pipeline
            .apply("createGrouped", Create.of(1, 2, 3))
            .apply("toKv", ParDo.of(new ToKvFn()))
            .apply("group", GroupByKey.create())
            .apply("ungroup", ParDo.of(new UngroupFn()));

    // Straight from a source, so this branch is a single instance.
    PCollection<Integer> direct = pipeline.apply("createDirect", Create.of(4, 5, 6));

    PCollectionList.of(shuffled).and(direct).apply("merge", Flatten.pCollections());
    return pipeline;
  }

  /** Every processor node in the topology, across all subtopologies. */
  private static List<String> processorNames(TopologyDescription description) {
    List<String> names = new ArrayList<>();
    for (TopologyDescription.Subtopology subtopology : description.subtopologies()) {
      for (TopologyDescription.Node node : subtopology.nodes()) {
        if (node instanceof TopologyDescription.Processor) {
          names.add(node.name());
        }
      }
    }
    return names;
  }

  private static void assertFlattenWasFusedAway(TopologyDescription description) {
    // No node stands for the Flatten. If one did, it would be wired to both branches and so would
    // run over a merged subtopology whose smaller-parallelism parent could not reach all of its
    // instances.
    for (String name : processorNames(description)) {
      assertThat(
          "no processor node should stand for the Flatten, but found " + name,
          name.contains(FLATTEN_NAME),
          is(false));
    }
    // The branches stay in separate subtopologies for the same reason: the source-fed branch, the
    // one behind the shuffle, and the second source-fed branch.
    assertThat(description.subtopologies().size(), is(3));
  }

  @Test
  public void branchesAtDifferentParallelismsAreFusedRatherThanLeftToTheRunner() {
    assertFlattenWasFusedAway(
        KafkaStreamsTestRunner.translate(mixedParallelismFlatten(4)).getTopology().describe());
  }

  @Test
  public void theSameHoldsAtASingleParallelism() {
    // Whether the Flatten is fused is a property of the fused graph, not of the parallelism, so
    // the shape is the same either way — which is why raising the parallelism cannot introduce a
    // Flatten node over mismatched branches.
    assertFlattenWasFusedAway(
        KafkaStreamsTestRunner.translate(mixedParallelismFlatten(1)).getTopology().describe());
  }
}
