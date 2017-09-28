/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.jstorm.translation;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.Map;

import org.apache.beam.runners.jstorm.JStormPipelineOptions;
import org.apache.beam.runners.jstorm.TestJStormRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the translation from PTransform to JStorm task.
 */
@RunWith(JUnit4.class)
public class PipelineTranslateTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  private Pipeline createPipeline() {
    PipelineOptions options = PipelineOptionsFactory.as(JStormPipelineOptions.class);
    options.setRunner(TestJStormRunner.class);
    return Pipeline.create(options);
  }

  private class TestPTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {

    private Pipeline p;

    public TestPTransform(Pipeline p) {
      this.p = p;
    }

    @Override
    public PCollection<Integer> expand(PCollection<Integer> input) {
      PCollection<Integer> pCollection = PCollection.createPrimitiveOutputInternal(
          p, null, PCollection.IsBounded.BOUNDED);
      pCollection.setCoder(input.getCoder());
      return pCollection;
    }
  }

  private class TestDoFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(ProcessContext context) {

    }
  }

  private class TestKvDoFn extends DoFn<Integer, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(KV.of("a", context.element()));
    }
  }

  @Test
  public void testSimplePipelineTranslation() {
    JStormPipelineOptions options = PipelineOptionsFactory.as(JStormPipelineOptions.class);
    Pipeline pipeline = createPipeline();
    pipeline.apply(Create.of(1))
        .apply(ParDo.of(new TestDoFn()));

    TranslationContext context = new TranslationContext(options);
    JStormPipelineTranslator transformer = new JStormPipelineTranslator(context);
    transformer.translate(pipeline);

    // Source is translated to spout, and ParDo is translated to bolt.
    TranslationContext.ExecutionGraphContext executionGraphContext =
        context.getExecutionGraphContext();
    assertEquals(1, executionGraphContext.getSpouts().size());
    assertEquals(1, executionGraphContext.getBolts().size());
    assertEquals(1, Iterables.size(executionGraphContext.getStreams()));

    // Check grouping type between spout and bolt.
    String spoutName = Iterables.getOnlyElement(executionGraphContext.getSpouts().keySet());
    String boltName = Iterables.getOnlyElement(executionGraphContext.getBolts().keySet());
    Stream stream = Iterables.getOnlyElement(executionGraphContext.getStreams());
    assertEquals(spoutName, stream.getProducer().getComponentId());
    assertEquals(boltName, stream.getConsumer().getComponentId());
    assertEquals(Stream.Grouping.Type.LOCAL_OR_SHUFFLE,
        stream.getConsumer().getGrouping().getType());
  }

  @Test
  public void testMultiParDoTranslation() {
    JStormPipelineOptions options = PipelineOptionsFactory.as(JStormPipelineOptions.class);
    Pipeline pipeline = createPipeline();
    pipeline.apply(Create.of(1))
        .apply("ParDo-1", ParDo.of(new TestDoFn()))
        .apply("ParDo-2", ParDo.of(new TestDoFn()));

    TranslationContext context = new TranslationContext(options);
    JStormPipelineTranslator transformer = new JStormPipelineTranslator(context);
    transformer.translate(pipeline);

    // Source is translated to spout, and the tow ParDos are chained and translated to one bolt.
    TranslationContext.ExecutionGraphContext executionGraphContext =
        context.getExecutionGraphContext();
    assertEquals(1, executionGraphContext.getSpouts().size());
    assertEquals(1, executionGraphContext.getBolts().size());
    assertEquals(1, Iterables.size(executionGraphContext.getStreams()));

    // Check grouping type between spout and bolt
    String spoutName = Iterables.getOnlyElement(executionGraphContext.getSpouts().keySet());
    String boltName = Iterables.getOnlyElement(executionGraphContext.getBolts().keySet());
    Stream stream = Iterables.getOnlyElement(executionGraphContext.getStreams());
    assertEquals(spoutName, stream.getProducer().getComponentId());
    assertEquals(boltName, stream.getConsumer().getComponentId());
    assertEquals(Stream.Grouping.Type.LOCAL_OR_SHUFFLE,
        stream.getConsumer().getGrouping().getType());
  }

  @Test
  public void testGroupByKeyTranslation() {
    JStormPipelineOptions options = PipelineOptionsFactory.as(JStormPipelineOptions.class);
    Pipeline pipeline = createPipeline();
    pipeline.apply("Source", Create.of(1))
        .apply("ParDo", ParDo.of(new TestKvDoFn()))
        .apply("GBK", GroupByKey.<String, Integer>create());

    TranslationContext context = new TranslationContext(options);
    JStormPipelineTranslator transformer = new JStormPipelineTranslator(context);
    transformer.translate(pipeline);

    // Source is translated to spout, ParDo and GBK are not chained and translated to two bolts.
    TranslationContext.ExecutionGraphContext executionGraphContext =
        context.getExecutionGraphContext();
    Map<String, UnboundedSourceSpout> spouts = executionGraphContext.getSpouts();
    Map<String, ExecutorsBolt> bolts = executionGraphContext.getBolts();
    Iterable<Stream> streams = executionGraphContext.getStreams();
    assertEquals(1, spouts.size());
    assertEquals(2, bolts.size());
    assertEquals(2, Iterables.size(streams));

    // Check grouping type between ParDo and GBK bolts
    for (Stream stream: streams) {
      if (bolts.containsKey(stream.getProducer().getComponentId())) {
          assertEquals(Stream.Grouping.Type.FIELDS,
                  stream.getConsumer().getGrouping().getType());
      }
    }
  }

  @Test
  public void testUnsupportedTransform() {
    Pipeline pipeline = createPipeline();
    pipeline.apply(Create.of(1))
        .apply(new TestPTransform(pipeline));

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("not supported");
    pipeline.run();
  }
}
