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
package org.apache.beam.sdk.runners;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
  public void testGetAggregatorStepsWithParDoBoundExtractsSteps() {
    @SuppressWarnings("rawtypes")
    ParDo.Bound bound = mock(ParDo.Bound.class, "Bound");
    AggregatorProvidingDoFn<ThreadGroup, StrictMath> fn = new AggregatorProvidingDoFn<>();
    when(bound.getFn()).thenReturn(fn);

    Aggregator<Long, Long> aggregatorOne = fn.addAggregator(new Sum.SumLongFn());
    Aggregator<Integer, Integer> aggregatorTwo = fn.addAggregator(new Min.MinIntegerFn());

    TransformTreeNode transformNode = mock(TransformTreeNode.class);
    when(transformNode.getTransform()).thenReturn(bound);

    doAnswer(new VisitNodesAnswer(ImmutableList.of(transformNode)))
        .when(p)
        .traverseTopologically(Mockito.any(PipelineVisitor.class));

    AggregatorPipelineExtractor extractor = new AggregatorPipelineExtractor(p);

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        extractor.getAggregatorSteps();

    assertEquals(ImmutableSet.<PTransform<?, ?>>of(bound), aggregatorSteps.get(aggregatorOne));
    assertEquals(ImmutableSet.<PTransform<?, ?>>of(bound), aggregatorSteps.get(aggregatorTwo));
    assertEquals(aggregatorSteps.size(), 2);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAggregatorStepsWithParDoBoundMultiExtractsSteps() {
    @SuppressWarnings("rawtypes")
    ParDo.BoundMulti bound = mock(ParDo.BoundMulti.class, "BoundMulti");
    AggregatorProvidingDoFn<Object, Void> fn = new AggregatorProvidingDoFn<>();
    when(bound.getFn()).thenReturn(fn);

    Aggregator<Long, Long> aggregatorOne = fn.addAggregator(new Max.MaxLongFn());
    Aggregator<Double, Double> aggregatorTwo = fn.addAggregator(new Min.MinDoubleFn());

    TransformTreeNode transformNode = mock(TransformTreeNode.class);
    when(transformNode.getTransform()).thenReturn(bound);

    doAnswer(new VisitNodesAnswer(ImmutableList.of(transformNode)))
        .when(p)
        .traverseTopologically(Mockito.any(PipelineVisitor.class));

    AggregatorPipelineExtractor extractor = new AggregatorPipelineExtractor(p);

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        extractor.getAggregatorSteps();

    assertEquals(ImmutableSet.<PTransform<?, ?>>of(bound), aggregatorSteps.get(aggregatorOne));
    assertEquals(ImmutableSet.<PTransform<?, ?>>of(bound), aggregatorSteps.get(aggregatorTwo));
    assertEquals(2, aggregatorSteps.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAggregatorStepsWithOneAggregatorInMultipleStepsAddsSteps() {
    @SuppressWarnings("rawtypes")
    ParDo.Bound bound = mock(ParDo.Bound.class, "Bound");
    @SuppressWarnings("rawtypes")
    ParDo.BoundMulti otherBound = mock(ParDo.BoundMulti.class, "otherBound");
    AggregatorProvidingDoFn<String, Math> fn = new AggregatorProvidingDoFn<>();
    when(bound.getFn()).thenReturn(fn);
    when(otherBound.getFn()).thenReturn(fn);

    Aggregator<Long, Long> aggregatorOne = fn.addAggregator(new Sum.SumLongFn());
    Aggregator<Double, Double> aggregatorTwo = fn.addAggregator(new Min.MinDoubleFn());

    TransformTreeNode transformNode = mock(TransformTreeNode.class);
    when(transformNode.getTransform()).thenReturn(bound);
    TransformTreeNode otherTransformNode = mock(TransformTreeNode.class);
    when(otherTransformNode.getTransform()).thenReturn(otherBound);

    doAnswer(new VisitNodesAnswer(ImmutableList.of(transformNode, otherTransformNode)))
        .when(p)
        .traverseTopologically(Mockito.any(PipelineVisitor.class));

    AggregatorPipelineExtractor extractor = new AggregatorPipelineExtractor(p);

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        extractor.getAggregatorSteps();

    assertEquals(
        ImmutableSet.<PTransform<?, ?>>of(bound, otherBound), aggregatorSteps.get(aggregatorOne));
    assertEquals(
        ImmutableSet.<PTransform<?, ?>>of(bound, otherBound), aggregatorSteps.get(aggregatorTwo));
    assertEquals(2, aggregatorSteps.size());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAggregatorStepsWithDifferentStepsAddsSteps() {
    @SuppressWarnings("rawtypes")
    ParDo.Bound bound = mock(ParDo.Bound.class, "Bound");

    AggregatorProvidingDoFn<ThreadGroup, Void> fn = new AggregatorProvidingDoFn<>();
    Aggregator<Long, Long> aggregatorOne = fn.addAggregator(new Sum.SumLongFn());

    when(bound.getFn()).thenReturn(fn);

    @SuppressWarnings("rawtypes")
    ParDo.BoundMulti otherBound = mock(ParDo.BoundMulti.class, "otherBound");

    AggregatorProvidingDoFn<Long, Long> otherFn = new AggregatorProvidingDoFn<>();
    Aggregator<Double, Double> aggregatorTwo = otherFn.addAggregator(new Sum.SumDoubleFn());

    when(otherBound.getFn()).thenReturn(otherFn);

    TransformTreeNode transformNode = mock(TransformTreeNode.class);
    when(transformNode.getTransform()).thenReturn(bound);
    TransformTreeNode otherTransformNode = mock(TransformTreeNode.class);
    when(otherTransformNode.getTransform()).thenReturn(otherBound);

    doAnswer(new VisitNodesAnswer(ImmutableList.of(transformNode, otherTransformNode)))
        .when(p)
        .traverseTopologically(Mockito.any(PipelineVisitor.class));

    AggregatorPipelineExtractor extractor = new AggregatorPipelineExtractor(p);

    Map<Aggregator<?, ?>, Collection<PTransform<?, ?>>> aggregatorSteps =
        extractor.getAggregatorSteps();

    assertEquals(ImmutableSet.<PTransform<?, ?>>of(bound), aggregatorSteps.get(aggregatorOne));
    assertEquals(ImmutableSet.<PTransform<?, ?>>of(otherBound), aggregatorSteps.get(aggregatorTwo));
    assertEquals(2, aggregatorSteps.size());
  }

  private static class VisitNodesAnswer implements Answer<Object> {
    private final List<TransformTreeNode> nodes;

    public VisitNodesAnswer(List<TransformTreeNode> nodes) {
      this.nodes = nodes;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      PipelineVisitor visitor = (PipelineVisitor) invocation.getArguments()[0];
      for (TransformTreeNode node : nodes) {
        visitor.visitPrimitiveTransform(node);
      }
      return null;
    }
  }

  private static class AggregatorProvidingDoFn<InT, OuT> extends OldDoFn<InT, OuT> {
    public <InputT, OutT> Aggregator<InputT, OutT> addAggregator(
        CombineFn<InputT, ?, OutT> combiner) {
      return createAggregator(randomName(), combiner);
    }

    private String randomName() {
      return UUID.randomUUID().toString();
    }

    @Override
    public void processElement(OldDoFn<InT, OuT>.ProcessContext c) throws Exception {
      fail();
    }
  }
}
