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
package org.apache.beam.runners.direct.portable;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasValue;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.GreedyPipelineFuser;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PortableGraph}. */
@RunWith(JUnit4.class)
public class PortableGraphTest implements Serializable {
  @Test
  public void getRootTransformsSucceeds() {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply("impulse", Impulse.create());
    pipeline.apply("otherImpulse", Impulse.create());

    PortableGraph graph = PortableGraph.forPipeline(PipelineTranslation.toProto(pipeline));
    assertThat(graph.getRootTransforms(), hasSize(2));

    assertThat(
        graph.getRootTransforms().stream().map(PTransformNode::getId).collect(Collectors.toSet()),
        containsInAnyOrder("impulse", "otherImpulse"));
  }

  @Test
  public void getExecutablesReturnsTransforms() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply("Impulse", Impulse.create())
        .apply(
            "ParDo",
            ParDo.of(
                new DoFn<byte[], KV<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctxt) {
                    ctxt.output(KV.of("foo", "bar"));
                  }
                }))
        .apply(GroupByKey.create())
        .apply(Values.create());

    PortableGraph graph = PortableGraph.forPipeline(PipelineTranslation.toProto(pipeline));
    assertThat(graph.getExecutables(), hasSize(4));
  }

  @Test
  public void getExecutablesWithStages() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply("Impulse", Impulse.create())
        .apply(
            "ParDo",
            ParDo.of(
                new DoFn<byte[], KV<String, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctxt) {
                    ctxt.output(KV.of("foo", "bar"));
                  }
                }))
        .apply(
            MapElements.into(new TypeDescriptor<KV<String, Integer>>() {})
                .via(input -> KV.of(input.getKey(), input.getValue().hashCode())))
        .apply(GroupByKey.create())
        .apply(Values.create());

    RunnerApi.Pipeline proto = PipelineTranslation.toProto(pipeline);
    RunnerApi.Pipeline fused = GreedyPipelineFuser.fuse(proto).toPipeline();
    PortableGraph graph = PortableGraph.forPipeline(fused);
    assertThat(graph.getExecutables(), hasSize(4));

    Stream<FunctionSpec> specStream =
        graph.getExecutables().stream().map(PTransformNode::getTransform).map(PTransform::getSpec);

    List<String> urns = specStream.map(FunctionSpec::getUrn).collect(Collectors.toList());
    assertThat(
        urns,
        containsInAnyOrder(
            PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
            PTransformTranslation.IMPULSE_TRANSFORM_URN,
            ExecutableStage.URN,
            ExecutableStage.URN));
  }

  @Test
  public void getProducedAndGetProducerSucceed() {
    Pipeline pipeline = Pipeline.create();
    TupleTag<KV<String, String>> mainTag = new TupleTag<>();
    TupleTag<Long> otherTag = new TupleTag<Long>() {};
    pipeline
        .apply("Impulse", Impulse.create())
        .apply(
            "ParDo",
            ParDo.of(
                    new DoFn<byte[], KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext ctxt) {
                        ctxt.output(KV.of("foo", "bar"));
                      }
                    })
                .withOutputTags(mainTag, TupleTagList.of(otherTag)))
        .get(mainTag)
        .apply(
            MapElements.into(new TypeDescriptor<KV<String, Integer>>() {})
                .via(input -> KV.of(input.getKey(), Objects.hash(input.getValue()))))
        .apply("gbk", GroupByKey.create())
        .apply("vals", Values.create());

    RunnerApi.Pipeline proto = PipelineTranslation.toProto(pipeline);
    PortableGraph graph = PortableGraph.forPipeline(proto);

    PTransformNode gbkNode =
        PipelineNode.pTransform("gbk", proto.getComponents().getTransformsOrThrow("gbk"));
    Collection<PCollectionNode> gbkOutput = graph.getProduced(gbkNode);
    assertThat(gbkOutput, hasSize(1));
    assertThat(graph.getProducer(getOnlyElement(gbkOutput)), equalTo(gbkNode));

    PTransformNode parDoNode =
        PipelineNode.pTransform("ParDo", proto.getComponents().getTransformsOrThrow("ParDo"));
    Collection<PCollectionNode> parDoOutput = graph.getProduced(parDoNode);
    assertThat(parDoOutput, hasSize(2));
    for (PCollectionNode parDoOutputNode : parDoOutput) {
      assertThat(graph.getProducer(parDoOutputNode), equalTo(parDoNode));
      assertThat(parDoNode.getTransform().getOutputsMap(), hasValue(parDoOutputNode.getId()));
    }
  }
}
