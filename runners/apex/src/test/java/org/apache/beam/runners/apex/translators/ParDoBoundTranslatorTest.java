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

package org.apache.beam.runners.apex.translators;

import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunnerResult;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.translators.functions.ApexParDoOperator;
import org.apache.beam.runners.apex.translators.io.ApexReadUnboundedInputOperator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.datatorrent.api.DAG;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

/**
 * integration test for {@link ParDoBoundTranslator}.
 */
@RunWith(JUnit4.class)
public class ParDoBoundTranslatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(ParDoBoundTranslatorTest.class);

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

    ApexRunnerResult result = (ApexRunnerResult)p.run();
    DAG dag = result.getApexDAG();

    DAG.OperatorMeta om = dag.getOperatorMeta("Create.Values");
    Assert.assertNotNull(om);
    Assert.assertEquals(om.getOperator().getClass(), ApexReadUnboundedInputOperator.class);

    om = dag.getOperatorMeta("ParDo(Add)");
    Assert.assertNotNull(om);
    Assert.assertEquals(om.getOperator().getClass(), ApexParDoOperator.class);

    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout) {
      if (EmbeddedCollector.results.containsAll(expected)) {
        break;
      }
      LOG.info("Waiting for expected results.");
      Thread.sleep(1000);
    }
    Assert.assertEquals(Sets.newHashSet(expected), EmbeddedCollector.results);
  }

  @SuppressWarnings("serial")
  private static class Add extends OldDoFn<Integer, Integer> {
    private final Integer number;

    public Add(Integer number) {
      this.number = number;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      c.output(c.element() + number);
    }
  }

  @SuppressWarnings("serial")
  private static class EmbeddedCollector extends OldDoFn<Object, Void> {
    protected static final HashSet<Object> results = new HashSet<>();

    public EmbeddedCollector() {
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      results.add(c.element());
    }
  }


  @Ignore
  @Test
  public void testAssertionFailure() throws Exception {
    ApexPipelineOptions options = PipelineOptionsFactory.create()
        .as(ApexPipelineOptions.class);
    options.setRunner(ApexRunner.class);
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

}
