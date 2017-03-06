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

package org.apache.beam.runners.apex.translation;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Sink;
import com.datatorrent.lib.util.KryoCloneUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.ApexRunnerResult;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.runners.apex.translation.operators.ApexParDoOperator;
import org.apache.beam.runners.apex.translation.operators.ApexReadUnboundedInputOperator;
import org.apache.beam.runners.apex.translation.utils.ApexStateInternals;
import org.apache.beam.runners.apex.translation.utils.ApexStreamTuple;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * integration test for {@link ParDoBoundTranslator}.
 */
@RunWith(JUnit4.class)
public class ParDoBoundTranslatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(ParDoBoundTranslatorTest.class);
  private static final long SLEEP_MILLIS = 500;
  private static final long TIMEOUT_MILLIS = 30000;

  @Test
  public void test() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create()
        .as(ApexPipelineOptions.class);
    options.setApplicationName("ParDoBound");
    options.setRunner(ApexRunner.class);

    Pipeline p = Pipeline.create(options);

    List<Integer> collection = Lists.newArrayList(1, 2, 3, 4, 5);
    List<Integer> expected = Lists.newArrayList(6, 7, 8, 9, 10);
    p.apply(Create.of(collection).withCoder(SerializableCoder.of(Integer.class)))
        .apply(ParDo.of(new Add(5)))
        .apply(ParDo.of(new EmbeddedCollector()));

    ApexRunnerResult result = (ApexRunnerResult) p.run();
    DAG dag = result.getApexDAG();

    DAG.OperatorMeta om = dag.getOperatorMeta("Create.Values");
    Assert.assertNotNull(om);
    Assert.assertEquals(om.getOperator().getClass(), ApexReadUnboundedInputOperator.class);

    om = dag.getOperatorMeta("ParDo(Add)");
    Assert.assertNotNull(om);
    Assert.assertEquals(om.getOperator().getClass(), ApexParDoOperator.class);

    long timeout = System.currentTimeMillis() + TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < timeout) {
      if (EmbeddedCollector.RESULTS.containsAll(expected)) {
        break;
      }
      LOG.info("Waiting for expected results.");
      Thread.sleep(SLEEP_MILLIS);
    }
    Assert.assertEquals(Sets.newHashSet(expected), EmbeddedCollector.RESULTS);
  }

  private static class Add extends DoFn<Integer, Integer> {
    private static final long serialVersionUID = 1L;
    private Integer number;
    private PCollectionView<Integer> sideInputView;

    private Add(Integer number) {
      this.number = number;
    }

    private Add(PCollectionView<Integer> sideInputView) {
      this.sideInputView = sideInputView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      if (sideInputView != null) {
        number = c.sideInput(sideInputView);
      }
      c.output(c.element() + number);
    }
  }

  private static class EmbeddedCollector extends DoFn<Object, Void> {
    private static final long serialVersionUID = 1L;
    private static final Set<Object> RESULTS = Collections.synchronizedSet(new HashSet<>());

    public EmbeddedCollector() {
      RESULTS.clear();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      RESULTS.add(c.element());
    }
  }

  private static Throwable runExpectingAssertionFailure(Pipeline pipeline) {
    // We cannot use thrown.expect(AssertionError.class) because the AssertionError
    // is first caught by JUnit and causes a test failure.
    try {
      pipeline.run();
    } catch (AssertionError exc) {
      return exc;
    }
    fail("assertion should have failed");
    throw new RuntimeException("unreachable");
  }

  @Test
  public void testAssertionFailure() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create()
        .as(ApexPipelineOptions.class);
    options.setRunner(TestApexRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> pcollection = pipeline
        .apply(Create.of(1, 2, 3, 4));
    PAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3, 7);

    Throwable exc = runExpectingAssertionFailure(pipeline);
    Pattern expectedPattern = Pattern.compile(
        "Expected: iterable over \\[((<4>|<7>|<3>|<2>|<1>)(, )?){5}\\] in any order");
    // A loose pattern, but should get the job done.
    assertTrue(
        "Expected error message from PAssert with substring matching "
            + expectedPattern
            + " but the message was \""
            + exc.getMessage()
            + "\"",
        expectedPattern.matcher(exc.getMessage()).find());
  }

  @Test
  public void testContainsInAnyOrder() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create().as(ApexPipelineOptions.class);
    options.setRunner(TestApexRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    PCollection<Integer> pcollection = pipeline.apply(Create.of(1, 2, 3, 4));
    PAssert.that(pcollection).containsInAnyOrder(2, 1, 4, 3);
    // TODO: terminate faster based on processed assertion vs. auto-shutdown
    pipeline.run();
  }

  @Test
  public void testSerialization() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create()
        .as(ApexPipelineOptions.class);
    options.setRunner(TestApexRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    Coder<WindowedValue<Integer>> coder = WindowedValue.getValueOnlyCoder(VarIntCoder.of());

    PCollectionView<Integer> singletonView = pipeline.apply(Create.of(1))
            .apply(Sum.integersGlobally().asSingletonView());

    ApexParDoOperator<Integer, Integer> operator =
        new ApexParDoOperator<>(
            options,
            new Add(singletonView),
            new TupleTag<Integer>(),
            TupleTagList.empty().getAll(),
            WindowingStrategy.globalDefault(),
            Collections.<PCollectionView<?>>singletonList(singletonView),
            coder,
            new ApexStateInternals.ApexStateInternalsFactory<Void>());
    operator.setup(null);
    operator.beginWindow(0);
    WindowedValue<Integer> wv1 = WindowedValue.valueInGlobalWindow(1);
    WindowedValue<Iterable<?>> sideInput = WindowedValue.<Iterable<?>>valueInGlobalWindow(
        Lists.<Integer>newArrayList(22));
    operator.input.process(ApexStreamTuple.DataTuple.of(wv1)); // pushed back input

    final List<Object> results = Lists.newArrayList();
    Sink<Object> sink =  new Sink<Object>() {
      @Override
      public void put(Object tuple) {
        results.add(tuple);
      }
      @Override
      public int getCount(boolean reset) {
        return 0;
      }
    };

    // verify pushed back input checkpointing
    Assert.assertNotNull("Serialization", operator = KryoCloneUtils.cloneObject(operator));
    operator.output.setSink(sink);
    operator.setup(null);
    operator.beginWindow(1);
    WindowedValue<Integer> wv2 = WindowedValue.valueInGlobalWindow(2);
    operator.sideInput1.process(ApexStreamTuple.DataTuple.of(sideInput));
    Assert.assertEquals("number outputs", 1, results.size());
    Assert.assertEquals("result", WindowedValue.valueInGlobalWindow(23),
        ((ApexStreamTuple.DataTuple<?>) results.get(0)).getValue());

    // verify side input checkpointing
    results.clear();
    Assert.assertNotNull("Serialization", operator = KryoCloneUtils.cloneObject(operator));
    operator.output.setSink(sink);
    operator.setup(null);
    operator.beginWindow(2);
    operator.input.process(ApexStreamTuple.DataTuple.of(wv2));
    Assert.assertEquals("number outputs", 1, results.size());
    Assert.assertEquals("result", WindowedValue.valueInGlobalWindow(24),
        ((ApexStreamTuple.DataTuple<?>) results.get(0)).getValue());
  }

  @Test
  public void testMultiOutputParDoWithSideInputs() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create().as(ApexPipelineOptions.class);
    options.setRunner(ApexRunner.class); // non-blocking run
    Pipeline pipeline = Pipeline.create(options);

    List<Integer> inputs = Arrays.asList(3, -42, 666);
    final TupleTag<String> mainOutputTag = new TupleTag<>("main");
    final TupleTag<Void> sideOutputTag = new TupleTag<>("sideOutput");

    PCollectionView<Integer> sideInput1 = pipeline
        .apply("CreateSideInput1", Create.of(11))
        .apply("ViewSideInput1", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInputUnread = pipeline
        .apply("CreateSideInputUnread", Create.of(-3333))
        .apply("ViewSideInputUnread", View.<Integer>asSingleton());
    PCollectionView<Integer> sideInput2 = pipeline
        .apply("CreateSideInput2", Create.of(222))
        .apply("ViewSideInput2", View.<Integer>asSingleton());

    PCollectionTuple outputs = pipeline
        .apply(Create.of(inputs))
        .apply(ParDo.withSideInputs(sideInput1)
            .withSideInputs(sideInputUnread)
            .withSideInputs(sideInput2)
            .withOutputTags(mainOutputTag, TupleTagList.of(sideOutputTag))
            .of(new TestMultiOutputWithSideInputsFn(
                Arrays.asList(sideInput1, sideInput2),
                Arrays.<TupleTag<String>>asList())));

    outputs.get(mainOutputTag).apply(ParDo.of(new EmbeddedCollector()));
    outputs.get(sideOutputTag).setCoder(VoidCoder.of());
    ApexRunnerResult result = (ApexRunnerResult) pipeline.run();

    HashSet<String> expected = Sets.newHashSet("processing: 3: [11, 222]",
        "processing: -42: [11, 222]", "processing: 666: [11, 222]");
    long timeout = System.currentTimeMillis() + TIMEOUT_MILLIS;
    while (System.currentTimeMillis() < timeout) {
      if (EmbeddedCollector.RESULTS.containsAll(expected)) {
        break;
      }
      LOG.info("Waiting for expected results.");
      Thread.sleep(SLEEP_MILLIS);
    }
    result.cancel();
    Assert.assertEquals(Sets.newHashSet(expected), EmbeddedCollector.RESULTS);
  }

  private static class TestMultiOutputWithSideInputsFn extends DoFn<Integer, String> {
    private static final long serialVersionUID = 1L;

    final List<PCollectionView<Integer>> sideInputViews = new ArrayList<>();
    final List<TupleTag<String>> sideOutputTupleTags = new ArrayList<>();

    public TestMultiOutputWithSideInputsFn(List<PCollectionView<Integer>> sideInputViews,
        List<TupleTag<String>> sideOutputTupleTags) {
      this.sideInputViews.addAll(sideInputViews);
      this.sideOutputTupleTags.addAll(sideOutputTupleTags);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      outputToAllWithSideInputs(c, "processing: " + c.element());
    }

    private void outputToAllWithSideInputs(ProcessContext c, String value) {
      if (!sideInputViews.isEmpty()) {
        List<Integer> sideInputValues = new ArrayList<>();
        for (PCollectionView<Integer> sideInputView : sideInputViews) {
          sideInputValues.add(c.sideInput(sideInputView));
        }
        value += ": " + sideInputValues;
      }
      c.output(value);
      for (TupleTag<String> sideOutputTupleTag : sideOutputTupleTags) {
        c.sideOutput(sideOutputTupleTag,
                     sideOutputTupleTag.getId() + ": " + value);
      }
    }

  }

}
