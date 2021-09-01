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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.fail;

import java.util.Collections;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DoFnInstanceManagers}. */
@RunWith(JUnit4.class)
public class DoFnInstanceManagersTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static class TestFn extends DoFn<Object, Object> {
    boolean tornDown = false;

    @ProcessElement
    public void processElement(ProcessContext processContext) throws Exception {}

    @Teardown
    public void teardown() throws Exception {
      if (tornDown) {
        fail("Should not be torn down twice");
      }
      tornDown = true;
    }
  }

  private DoFn<?, ?> initialFn = new TestFn();
  private PipelineOptions options = PipelineOptionsFactory.create();

  @Test
  public void testInstanceReturnsInstance() throws Exception {
    DoFnInfo<?, ?> info =
        DoFnInfo.forFn(
            initialFn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            new TupleTag<>(PropertyNames.OUTPUT) /* main output id */,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    DoFnInstanceManager mgr = DoFnInstanceManagers.singleInstance(info);
    assertThat(mgr.peek(), Matchers.<DoFnInfo<?, ?>>theInstance(info));
    assertThat(mgr.get(), Matchers.<DoFnInfo<?, ?>>theInstance(info));

    // This call should be identical to the above, and return the same object
    assertThat(mgr.get(), Matchers.<DoFnInfo<?, ?>>theInstance(info));
    // Peek should always return the same object
    assertThat(mgr.peek(), Matchers.<DoFnInfo<?, ?>>theInstance(info));
  }

  @Test
  public void testInstanceIgnoresAbort() throws Exception {
    DoFnInfo<?, ?> info =
        DoFnInfo.forFn(
            initialFn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            new TupleTag<>(PropertyNames.OUTPUT) /* main output id */,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    DoFnInstanceManager mgr = DoFnInstanceManagers.singleInstance(info);
    mgr.abort(mgr.get());
    // TestFn#teardown would fail the test after multiple calls
    mgr.abort(mgr.get());
    // The returned info is still the initial info
    assertThat(mgr.get(), Matchers.<DoFnInfo<?, ?>>theInstance(info));
    assertThat(mgr.get().getDoFn(), theInstance(initialFn));
  }

  @Test
  public void testCloningPoolReusesAfterComplete() throws Exception {
    DoFnInfo<?, ?> info =
        DoFnInfo.forFn(
            initialFn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            new TupleTag<>(PropertyNames.OUTPUT) /* main output id */,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    DoFnInstanceManager mgr = DoFnInstanceManagers.cloningPool(info, options);
    DoFnInfo<?, ?> retrievedInfo = mgr.get();
    assertThat(retrievedInfo, not(Matchers.<DoFnInfo<?, ?>>theInstance(info)));
    assertThat(retrievedInfo.getDoFn(), not(theInstance(info.getDoFn())));

    mgr.complete(retrievedInfo);
    DoFnInfo<?, ?> afterCompleteInfo = mgr.get();
    assertThat(afterCompleteInfo, Matchers.<DoFnInfo<?, ?>>theInstance(retrievedInfo));
    assertThat(afterCompleteInfo.getDoFn(), theInstance(retrievedInfo.getDoFn()));
  }

  @Test
  public void testCloningPoolTearsDownAfterAbort() throws Exception {
    DoFnInfo<?, ?> info =
        DoFnInfo.forFn(
            initialFn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            new TupleTag<>(PropertyNames.OUTPUT) /* main output id */,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    DoFnInstanceManager mgr = DoFnInstanceManagers.cloningPool(info, options);
    DoFnInfo<?, ?> retrievedInfo = mgr.get();

    mgr.abort(retrievedInfo);
    TestFn fn = (TestFn) retrievedInfo.getDoFn();
    assertThat(fn.tornDown, is(true));

    DoFnInfo<?, ?> afterAbortInfo = mgr.get();
    assertThat(afterAbortInfo, not(Matchers.<DoFnInfo<?, ?>>theInstance(retrievedInfo)));
    assertThat(afterAbortInfo.getDoFn(), not(theInstance(retrievedInfo.getDoFn())));
    assertThat(((TestFn) afterAbortInfo.getDoFn()).tornDown, is(false));
  }

  @Test
  public void testCloningPoolMultipleOutstanding() throws Exception {
    DoFnInfo<?, ?> info =
        DoFnInfo.forFn(
            initialFn,
            WindowingStrategy.globalDefault(),
            null /* side input views */,
            null /* input coder */,
            new TupleTag<>(PropertyNames.OUTPUT) /* main output id */,
            DoFnSchemaInformation.create(),
            Collections.emptyMap());

    DoFnInstanceManager mgr = DoFnInstanceManagers.cloningPool(info, options);

    DoFnInfo<?, ?> firstInfo = mgr.get();
    DoFnInfo<?, ?> secondInfo = mgr.get();

    assertThat(firstInfo, not(Matchers.<DoFnInfo<?, ?>>theInstance(secondInfo)));
    assertThat(firstInfo.getDoFn(), not(theInstance(secondInfo.getDoFn())));
  }
}
