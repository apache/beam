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

package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.base.Equivalence;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link PipelineTranslation}. */
@RunWith(Parameterized.class)
public class PipelineTranslationTest {
  @Parameter(0)
  public Pipeline pipeline;

  @Parameters(name = "{index}")
  public static Iterable<Pipeline> testPipelines() {
    Pipeline trivialPipeline = Pipeline.create();
    trivialPipeline.apply(Create.of(1, 2, 3));

    Pipeline sideInputPipeline = Pipeline.create();
    final PCollectionView<String> singletonView =
        sideInputPipeline.apply(Create.of("foo")).apply(View.<String>asSingleton());
    sideInputPipeline
        .apply(Create.of("main input"))
        .apply(
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        // actually never executed and no effect on translation
                        c.sideInput(singletonView);
                      }
                    })
                .withSideInputs(singletonView));

    Pipeline complexPipeline = Pipeline.create();
    BigEndianLongCoder customCoder = BigEndianLongCoder.of();
    PCollection<Long> elems = complexPipeline.apply(GenerateSequence.from(0L).to(207L));
    PCollection<Long> counted = elems.apply(Count.<Long>globally()).setCoder(customCoder);
    PCollection<Long> windowed =
        counted.apply(
            Window.<Long>into(FixedWindows.of(Duration.standardMinutes(7)))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(AfterPane.elementCountAtLeast(19)))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.standardMinutes(3L)));
    final WindowingStrategy<?, ?> windowedStrategy = windowed.getWindowingStrategy();
    PCollection<KV<String, Long>> keyed = windowed.apply(WithKeys.<String, Long>of("foo"));
    PCollection<KV<String, Iterable<Long>>> grouped =
        keyed.apply(GroupByKey.<String, Long>create());

    return ImmutableList.of(trivialPipeline, sideInputPipeline, complexPipeline);
  }

  @Test
  public void testProtoDirectly() {
    final RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);
    pipeline.traverseTopologically(
        new PipelineProtoVerificationVisitor(pipelineProto));
  }

  @Test
  public void testProtoAgainstRehydrated() throws Exception {
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline);
    Pipeline rehydrated = PipelineTranslation.fromProto(pipelineProto);

    rehydrated.traverseTopologically(
        new PipelineProtoVerificationVisitor(pipelineProto));
  }

  private static class PipelineProtoVerificationVisitor extends PipelineVisitor.Defaults {

    private final RunnerApi.Pipeline pipelineProto;
    Set<Node> transforms;
    Set<PCollection<?>> pcollections;
    Set<Equivalence.Wrapper<? extends Coder<?>>> coders;
    Set<WindowingStrategy<?, ?>> windowingStrategies;

    public PipelineProtoVerificationVisitor(RunnerApi.Pipeline pipelineProto) {
      this.pipelineProto = pipelineProto;
      transforms = new HashSet<>();
      pcollections = new HashSet<>();
      coders = new HashSet<>();
      windowingStrategies = new HashSet<>();
    }

    @Override
    public void leaveCompositeTransform(Node node) {
      if (node.isRootNode()) {
        assertThat(
            "Unexpected number of PTransforms",
            pipelineProto.getComponents().getTransformsCount(),
            equalTo(transforms.size()));
        assertThat(
            "Unexpected number of PCollections",
            pipelineProto.getComponents().getPcollectionsCount(),
            equalTo(pcollections.size()));
        assertThat(
            "Unexpected number of Coders",
            pipelineProto.getComponents().getCodersCount(),
            equalTo(coders.size()));
        assertThat(
            "Unexpected number of Windowing Strategies",
            pipelineProto.getComponents().getWindowingStrategiesCount(),
            equalTo(windowingStrategies.size()));
      } else {
        transforms.add(node);
        if (PTransformTranslation.COMBINE_TRANSFORM_URN.equals(
            PTransformTranslation.urnForTransformOrNull(node.getTransform()))) {
          // Combine translation introduces a coder that is not assigned to any PCollection
          // in the default expansion, and must be explicitly added here.
          try {
            addCoders(
                CombineTranslation.getAccumulatorCoder(node.toAppliedPTransform(getPipeline())));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    public void visitPrimitiveTransform(Node node) {
      transforms.add(node);
    }

    @Override
    public void visitValue(PValue value, Node producer) {
      if (value instanceof PCollection) {
        PCollection pc = (PCollection) value;
        pcollections.add(pc);
        addCoders(pc.getCoder());
        windowingStrategies.add(pc.getWindowingStrategy());
        addCoders(pc.getWindowingStrategy().getWindowFn().windowCoder());
      }
    }

    private void addCoders(Coder<?> coder) {
      coders.add(Equivalence.<Coder<?>>identity().wrap(coder));
      if (CoderTranslation.KNOWN_CODER_URNS.containsKey(coder.getClass())) {
        for (Coder<?> component : ((StructuredCoder<?>) coder).getComponents()) {
          addCoders(component);
        }
      }
    }
  }
}
