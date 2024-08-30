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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.Structs.addString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Collections;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DefaultParDoFnFactory}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class DefaultParDoFnFactoryTest {

  private static class TestDoFn extends DoFn<Integer, String> {
    final String stringField;
    final long longField;

    TestDoFn(String stringValue, long longValue) {
      this.stringField = stringValue;
      this.longField = longValue;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      // This is called to ensure the SimpleParDoFn is fully initialized
    }
  }

  // Miscellaneous default values required by the ParDoFnFactory interface
  private static final ParDoFnFactory DEFAULT_FACTORY = new DefaultParDoFnFactory();
  private static final PipelineOptions DEFAULT_OPTIONS = PipelineOptionsFactory.create();
  private static final DataflowExecutionContext<?> DEFAULT_EXECUTION_CONTEXT =
      BatchModeExecutionContext.forTesting(DEFAULT_OPTIONS, "testStage");
  private final CounterSet counterSet = new CounterSet();
  private static final TupleTag<?> MAIN_OUTPUT = new TupleTag<>("1");

  /**
   * Tests that a {@link SimpleParDoFn} is correctly dispatched to {@code UserParDoFnFactory} and
   * instantiated correctly.
   */
  @Test
  public void testCreateSimpleParDoFn() throws Exception {
    // A serialized DoFn
    String stringFieldValue = "some state";
    long longFieldValue = 42L;
    TestDoFn fn = new TestDoFn(stringFieldValue, longFieldValue);
    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                DoFnInfo.forFn(
                    fn,
                    WindowingStrategy.globalDefault(),
                    null /* side input views */,
                    null /* input coder */,
                    new TupleTag<>("output") /* main output */,
                    DoFnSchemaInformation.create(),
                    Collections.emptyMap())));
    CloudObject cloudUserFn = CloudObject.forClassName("DoFn");
    addString(cloudUserFn, "serialized_fn", serializedFn);

    // Create the ParDoFn from the serialized DoFn
    ParDoFn parDoFn =
        DEFAULT_FACTORY.create(
            DEFAULT_OPTIONS,
            cloudUserFn,
            null,
            MAIN_OUTPUT,
            ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
            DEFAULT_EXECUTION_CONTEXT,
            TestOperationContext.create(counterSet));

    // Test that the factory created the correct class
    assertThat(parDoFn, instanceOf(SimpleParDoFn.class));

    // TODO: move the asserts below into new tests in UserParDoFnFactoryTest, and this test should
    // simply assert that DefaultParDoFnFactory.create() matches UserParDoFnFactory.create()

    // Test that the DoFnInfo reflects the one passed in
    SimpleParDoFn simpleParDoFn = (SimpleParDoFn) parDoFn;
    parDoFn.startBundle(new OutputReceiver());
    // DoFnInfo may not yet be initialized until an element is processed
    parDoFn.processElement(WindowedValue.valueInGlobalWindow("foo"));
    @SuppressWarnings("rawtypes")
    DoFnInfo doFnInfo = simpleParDoFn.getDoFnInfo();
    DoFn innerDoFn = (TestDoFn) doFnInfo.getDoFn();
    assertThat(innerDoFn, instanceOf(TestDoFn.class));
    assertThat(doFnInfo.getWindowingStrategy().getWindowFn(), instanceOf(GlobalWindows.class));
    assertThat(doFnInfo.getWindowingStrategy().getTrigger(), instanceOf(DefaultTrigger.class));

    // Test that the deserialized user DoFn is as expected
    TestDoFn actualTestDoFn = (TestDoFn) innerDoFn;
    assertEquals(stringFieldValue, actualTestDoFn.stringField);
    assertEquals(longFieldValue, actualTestDoFn.longField);
  }

  @Test
  public void testCreateUnknownParDoFn() throws Exception {
    // A bogus serialized DoFn
    CloudObject cloudUserFn = CloudObject.forClassName("UnknownKindOfDoFn");
    try {
      DEFAULT_FACTORY.create(
          DEFAULT_OPTIONS,
          cloudUserFn,
          null,
          MAIN_OUTPUT,
          ImmutableMap.<TupleTag<?>, Integer>of(MAIN_OUTPUT, 0),
          DEFAULT_EXECUTION_CONTEXT,
          TestOperationContext.create(counterSet));
      fail("should have thrown an exception");
    } catch (Exception exn) {
      assertThat(exn.toString(), Matchers.containsString("No known ParDoFnFactory"));
    }
  }

  // TODO: Test side inputs.
}
