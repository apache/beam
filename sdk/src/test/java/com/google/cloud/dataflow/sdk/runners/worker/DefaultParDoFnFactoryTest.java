/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link DefaultParDoFnFactory}.
 */
@RunWith(JUnit4.class)
public class DefaultParDoFnFactoryTest {

  private static class TestDoFn extends DoFn<Integer, String> {
    final String stringField;
    final long longField;

    TestDoFn(String stringValue, long longValue) {
      this.stringField = stringValue;
      this.longField = longValue;
    }

    @Override
    public void processElement(ProcessContext c) {
      throw new RuntimeException("not expecting to call this");
    }
  }

  // Miscellaneous default values required by the ParDoFnFactory interface
  private static final ParDoFnFactory DEFAULT_FACTORY = new DefaultParDoFnFactory();
  private static final PipelineOptions DEFAULT_OPTIONS = PipelineOptionsFactory.create();
  private static final DataflowExecutionContext DEFAULT_EXECUTION_CONTEXT =
      BatchModeExecutionContext.fromOptions(DEFAULT_OPTIONS);
  private static final CounterSet EMPTY_COUNTER_SET = new CounterSet();
  private static final StateSampler EMPTY_STATE_SAMPLER =
      new StateSampler("test", EMPTY_COUNTER_SET.getAddCounterMutator());

  private List<MultiOutputInfo> dummySingleOutputInfo;

  @Before
  public void setUp() throws Exception {
    String tag = "output";
    MultiOutputInfo multiOutputInfo = new MultiOutputInfo();
    multiOutputInfo.setTag(tag);
    dummySingleOutputInfo = Collections.singletonList(multiOutputInfo);
  }

  /**
   * Tests that a "normal" {@link DoFn} is correctly dispatched to {@link NormalParDoFn} and
   * instantiated correctly.
   */
  @Test
  public void testCreateNormalParDoFn() throws Exception {
    // A serialized DoFn
    String stringFieldValue = "some state";
    long longFieldValue = 42L;
    TestDoFn fn = new TestDoFn(stringFieldValue, longFieldValue);
    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(
                new DoFnInfo<>(fn, WindowingStrategy.globalDefault())));
    CloudObject cloudUserFn = CloudObject.forClassName("DoFn");
    addString(cloudUserFn, "serialized_fn", serializedFn);

    // Create the ParDoFn from the serialized DoFn
    ParDoFn parDoFn = DEFAULT_FACTORY.create(
        DEFAULT_OPTIONS,
        cloudUserFn,
        "name",
        "transformName",
        null,
        dummySingleOutputInfo,
        1,
        DEFAULT_EXECUTION_CONTEXT,
        EMPTY_COUNTER_SET.getAddCounterMutator(),
        EMPTY_STATE_SAMPLER);

    // Test that the factory created the correct class
    assertThat(parDoFn, instanceOf(NormalParDoFn.class));

    // TODO: move the asserts below into new tests in NormalParDoFnTest, and this test should
    // simply assert that DefaultParDoFnFactory.create() matches NormalParDoFn.Factory.create()

    // Test that the DoFnInfo reflects the one passed in
    NormalParDoFn normalParDoFn = (NormalParDoFn) parDoFn;
    @SuppressWarnings("rawtypes")
    DoFnInfo doFnInfo = normalParDoFn.getDoFnInfo();
    @SuppressWarnings("rawtypes")
    DoFn actualDoFn = doFnInfo.getDoFn();
    assertThat(actualDoFn, instanceOf(TestDoFn.class));
    assertThat(
        doFnInfo.getWindowingStrategy().getWindowFn(),
        instanceOf(GlobalWindows.class));
    assertThat(
        doFnInfo.getWindowingStrategy().getTrigger().getSpec(),
        instanceOf(DefaultTrigger.class));

    // Test that the deserialized user DoFn is as expected
    TestDoFn actualTestDoFn = (TestDoFn) actualDoFn;
    assertEquals(stringFieldValue, actualTestDoFn.stringField);
    assertEquals(longFieldValue, actualTestDoFn.longField);
    assertEquals(DEFAULT_EXECUTION_CONTEXT, normalParDoFn.getExecutionContext());
  }

  @Test
  public void testCreateUnknownParDoFn() throws Exception {
    // A bogus serialized DoFn
    CloudObject cloudUserFn = CloudObject.forClassName("UnknownKindOfDoFn");
    try {
      DEFAULT_FACTORY.create(
          DEFAULT_OPTIONS,
          cloudUserFn,
          "name",
          "transformName",
          null,
          null,
          1,
          DEFAULT_EXECUTION_CONTEXT,
          EMPTY_COUNTER_SET.getAddCounterMutator(),
          EMPTY_STATE_SAMPLER);
      fail("should have thrown an exception");
    } catch (Exception exn) {
      assertThat(
          exn.toString(),
          Matchers.containsString("No known ParDoFnFactory"));
    }
  }

  // TODO: Test side inputs.
}
