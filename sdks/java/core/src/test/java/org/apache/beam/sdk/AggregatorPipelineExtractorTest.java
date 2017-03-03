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
package org.apache.beam.sdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link AggregatorPipelineExtractor}.
 */
@RunWith(JUnit4.class)
public class AggregatorPipelineExtractorTest {
  @Mock
  private Pipeline p;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAggregatorStepsWithParDoSingleOutputExtractsSteps() {
    @SuppressWarnings("rawtypes")
    ParDo.SingleOutput parDo = mock(ParDo.SingleOutput.class, "parDo");
    AggregatorProvidingDoFn<ThreadGroup, StrictMath> fn = new AggregatorProvidingDoFn<>();
    when(parDo.getFn()).thenReturn(fn);

    Aggregator<Long, Long> aggregatorOne = fn.addAggregator(Sum.ofLongs());
    Aggregator<Integer, Integer> aggregatorTwo = fn.addAggregator(Min.ofIntegers());

    TransformHierarchy.Node transformNode = mock(TransformHierarchy.Node.class);
    when(transformNode.getTransform()).thenReturn(parDo);

    doAnswer(new VisitNodesAnswer(ImmutableList.of(transformNode)))
        .when(p)
        .traverseTopologically(Mockito.any(PipelineVisitor.class));

    AggregatorPipelineExtractor extractor = new AggregatorPipelineExtractor(p);

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        extractor.getAggregatorSteps();

    assertEquals(ImmutableSet.<PTransform<?, ?>>of(parDo), aggregatorSteps.get(aggregatorOne));
    assertEquals(ImmutableSet.<PTransform<?, ?>>of(parDo), aggregatorSteps.get(aggregatorTwo));
    assertEquals(aggregatorSteps.size(), 2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAggregatorStepsWithParDoMultiOutputExtractsSteps() {
    @SuppressWarnings("rawtypes")
    ParDo.MultiOutput parDo = mock(ParDo.MultiOutput.class, "parDo");
    AggregatorProvidingDoFn<Object, Void> fn = new AggregatorProvidingDoFn<>();
    when(parDo.getFn()).thenReturn(fn);

    Aggregator<Long, Long> aggregatorOne = fn.addAggregator(Max.ofLongs());
    Aggregator<Double, Double> aggregatorTwo = fn.addAggregator(Min.ofDoubles());

    TransformHierarchy.Node transformNode = mock(TransformHierarchy.Node.class);
    when(transformNode.getTransform()).thenReturn(parDo);

    doAnswer(new VisitNodesAnswer(ImmutableList.of(transformNode)))
        .when(p)
        .traverseTopologically(Mockito.any(PipelineVisitor.class));

    AggregatorPipelineExtractor extractor = new AggregatorPipelineExtractor(p);

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        extractor.getAggregatorSteps();

    assertEquals(ImmutableSet.<PTransform<?, ?>>of(parDo), aggregatorSteps.get(aggregatorOne));
    assertEquals(ImmutableSet.<PTransform<?, ?>>of(parDo), aggregatorSteps.get(aggregatorTwo));
    assertEquals(2, aggregatorSteps.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAggregatorStepsWithOneAggregatorInMultipleStepsAddsSteps() {
    @SuppressWarnings("rawtypes")
    ParDo.SingleOutput parDo = mock(ParDo.SingleOutput.class, "parDo");
    @SuppressWarnings("rawtypes")
    ParDo.MultiOutput otherParDo = mock(ParDo.MultiOutput.class, "otherParDo");
    AggregatorProvidingDoFn<String, Math> fn = new AggregatorProvidingDoFn<>();
    when(parDo.getFn()).thenReturn(fn);
    when(otherParDo.getFn()).thenReturn(fn);

    Aggregator<Long, Long> aggregatorOne = fn.addAggregator(Sum.ofLongs());
    Aggregator<Double, Double> aggregatorTwo = fn.addAggregator(Min.ofDoubles());

    TransformHierarchy.Node transformNode = mock(TransformHierarchy.Node.class);
    when(transformNode.getTransform()).thenReturn(parDo);
    TransformHierarchy.Node otherTransformNode = mock(TransformHierarchy.Node.class);
    when(otherTransformNode.getTransform()).thenReturn(otherParDo);

    doAnswer(new VisitNodesAnswer(ImmutableList.of(transformNode, otherTransformNode)))
        .when(p)
        .traverseTopologically(Mockito.any(PipelineVisitor.class));

    AggregatorPipelineExtractor extractor = new AggregatorPipelineExtractor(p);

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        extractor.getAggregatorSteps();

    assertEquals(
        ImmutableSet.<PTransform<?, ?>>of(parDo, otherParDo), aggregatorSteps.get(aggregatorOne));
    assertEquals(
        ImmutableSet.<PTransform<?, ?>>of(parDo, otherParDo), aggregatorSteps.get(aggregatorTwo));
    assertEquals(2, aggregatorSteps.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAggregatorStepsWithDifferentStepsAddsSteps() {
    @SuppressWarnings("rawtypes")
    ParDo.SingleOutput parDo = mock(ParDo.SingleOutput.class, "parDo");

    AggregatorProvidingDoFn<ThreadGroup, Void> fn = new AggregatorProvidingDoFn<>();
    Aggregator<Long, Long> aggregatorOne = fn.addAggregator(Sum.ofLongs());

    when(parDo.getFn()).thenReturn(fn);

    @SuppressWarnings("rawtypes")
    ParDo.MultiOutput otherParDo = mock(ParDo.MultiOutput.class, "otherParDo");

    AggregatorProvidingDoFn<Long, Long> otherFn = new AggregatorProvidingDoFn<>();
    Aggregator<Double, Double> aggregatorTwo = otherFn.addAggregator(Sum.ofDoubles());

    when(otherParDo.getFn()).thenReturn(otherFn);

    TransformHierarchy.Node transformNode = mock(TransformHierarchy.Node.class);
    when(transformNode.getTransform()).thenReturn(parDo);
    TransformHierarchy.Node otherTransformNode = mock(TransformHierarchy.Node.class);
    when(otherTransformNode.getTransform()).thenReturn(otherParDo);

    doAnswer(new VisitNodesAnswer(ImmutableList.of(transformNode, otherTransformNode)))
        .when(p)
        .traverseTopologically(Mockito.any(PipelineVisitor.class));

    AggregatorPipelineExtractor extractor = new AggregatorPipelineExtractor(p);

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        extractor.getAggregatorSteps();

    assertEquals(ImmutableSet.<PTransform<?, ?>>of(parDo), aggregatorSteps.get(aggregatorOne));
    assertEquals(ImmutableSet.<PTransform<?, ?>>of(otherParDo), aggregatorSteps.get(aggregatorTwo));
    assertEquals(2, aggregatorSteps.size());
  }

  private static class VisitNodesAnswer implements Answer<Object> {
    private final List<TransformHierarchy.Node> nodes;

    public VisitNodesAnswer(List<TransformHierarchy.Node> nodes) {
      this.nodes = nodes;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      PipelineVisitor visitor = (PipelineVisitor) invocation.getArguments()[0];
      for (TransformHierarchy.Node node : nodes) {
        visitor.visitPrimitiveTransform(node);
      }
      return null;
    }
  }

  private static class AggregatorProvidingDoFn<InT, OuT> extends DoFn<InT, OuT> {
    public <InputT, OutT> Aggregator<InputT, OutT> addAggregator(
        CombineFn<InputT, ?, OutT> combiner) {
      return createAggregator(randomName(), combiner);
    }

    private String randomName() {
      return UUID.randomUUID().toString();
    }

    @ProcessElement
    public void processElement(DoFn<InT, OuT>.ProcessContext c) throws Exception {
      fail();
    }
  }
}
