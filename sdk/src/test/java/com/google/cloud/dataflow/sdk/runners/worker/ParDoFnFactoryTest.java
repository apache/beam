/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.StringUtils;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;

import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for ParDoFnFactory.
 */
@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "serial", "unchecked"})
public class ParDoFnFactoryTest {
  static class TestDoFn extends DoFn<Integer, String> {
    final String stringState;
    final long longState;

    TestDoFn(String stringState, long longState) {
      this.stringState = stringState;
      this.longState = longState;
    }

    @Override
    public void processElement(ProcessContext c) {
      throw new RuntimeException("not expecting to call this");
    }
  }

  @Test
  public void testCreateNormalParDoFn() throws Exception {
    String stringState = "some state";
    long longState = 42L;

    TestDoFn fn = new TestDoFn(stringState, longState);

    String serializedFn =
        StringUtils.byteArrayToJsonString(
            SerializableUtils.serializeToByteArray(new DoFnInfo(fn, new GlobalWindows())));

    CloudObject cloudUserFn = CloudObject.forClassName("DoFn");
    addString(cloudUserFn, "serialized_fn", serializedFn);

    String tag = "output";
    MultiOutputInfo multiOutputInfo = new MultiOutputInfo();
    multiOutputInfo.setTag(tag);
    List<MultiOutputInfo> multiOutputInfos =
        Arrays.asList(multiOutputInfo);

    BatchModeExecutionContext context = new BatchModeExecutionContext();
    CounterSet counters = new CounterSet();
    StateSampler stateSampler = new StateSampler(
        "test", counters.getAddCounterMutator());
    ParDoFn parDoFn = ParDoFnFactory.create(
        PipelineOptionsFactory.create(),
        cloudUserFn, "name", null, multiOutputInfos, 1,
        context, counters.getAddCounterMutator(), stateSampler);

    Assert.assertThat(parDoFn, new IsInstanceOf(NormalParDoFn.class));
    NormalParDoFn normalParDoFn = (NormalParDoFn) parDoFn;

    DoFn actualDoFn = normalParDoFn.fnFactory.createDoFnInfo().getDoFn();
    Assert.assertThat(actualDoFn, new IsInstanceOf(TestDoFn.class));
    Assert.assertThat(
        normalParDoFn.fnFactory.createDoFnInfo().getWindowFn(),
        new IsInstanceOf(GlobalWindows.class));
    TestDoFn actualTestDoFn = (TestDoFn) actualDoFn;

    Assert.assertEquals(stringState, actualTestDoFn.stringState);
    Assert.assertEquals(longState, actualTestDoFn.longState);

    Assert.assertEquals(context, normalParDoFn.executionContext);
  }

  @Test
  public void testCreateUnknownParDoFn() throws Exception {
    CloudObject cloudUserFn = CloudObject.forClassName("UnknownKindOfDoFn");
    try {
      CounterSet counters = new CounterSet();
      StateSampler stateSampler = new StateSampler(
          "test", counters.getAddCounterMutator());
      ParDoFnFactory.create(PipelineOptionsFactory.create(),
                            cloudUserFn, "name", null, null, 1,
                            new BatchModeExecutionContext(),
                            counters.getAddCounterMutator(),
                            stateSampler);
      Assert.fail("should have thrown an exception");
    } catch (Exception exn) {
      Assert.assertThat(exn.toString(),
                        CoreMatchers.containsString(
                            "unable to create a ParDoFn"));
    }
  }

  // TODO: Test side inputs.
}
