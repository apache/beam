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

package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SdkComponents}. */
@RunWith(JUnit4.class)
public class SdkComponentsTest {
  @Rule
  public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private SdkComponents components = SdkComponents.create();

  @Test
  public void getCoderId() {
    Coder<?> coder =
        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(SetCoder.of(ByteArrayCoder.of())));
    String id = components.getCoderId(coder);
    assertThat(components.getCoderId(coder), equalTo(id));
    assertThat(id, not(isEmptyOrNullString()));
    assertThat(components.getCoderId(VarLongCoder.of()), not(equalTo(id)));
  }

  @Test
  public void getTransformId() {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    String userName = "my_transform/my_nesting";
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            userName, pipeline.begin().expand(), pt.expand(), create, pipeline);
    String componentName = components.getTransformId(transform);
    assertThat(componentName, equalTo(userName));
    assertThat(components.getTransformId(transform), equalTo(componentName));
  }

  @Test
  public void getTransformIdEmptyFullName() {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            "", pipeline.begin().expand(), pt.expand(), create, pipeline);
    String assignedName = components.getTransformId(transform);

    assertThat(assignedName, not(isEmptyOrNullString()));
  }

  @Test
  public void getPCollectionId() {
    PCollection<Long> pCollection = pipeline.apply(CountingInput.unbounded()).setName("foo");
    String id = components.getPCollectionId(pCollection);
    assertThat(id, equalTo("foo"));
  }

  @Test
  public void putPCollectionExistingNameCollision() {
    PCollection<Long> pCollection =
        pipeline.apply("FirstCount", CountingInput.unbounded()).setName("foo");
    String firstId = components.getPCollectionId(pCollection);
    PCollection<Long> duplicate =
        pipeline.apply("SecondCount", CountingInput.unbounded()).setName("foo");
    String secondId = components.getPCollectionId(duplicate);
    assertThat(firstId, equalTo("foo"));
    assertThat(secondId, containsString("foo"));
    assertThat(secondId, not(equalTo("foo")));
  }

  @Test
  public void getWindowingStrategyId() {
    WindowingStrategy<?, ?> strategy =
        WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    String name = components.getWindowingStrategyId(strategy);
    assertThat(name, not(isEmptyOrNullString()));
  }

  // TODO: Determine if desired
  // @Test public void windowingStrategyGlobalDefault()

  @Test
  public void getWindowingStrategyIdEqualStrategies() {
    WindowingStrategy<?, ?> strategy =
        WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    String name = components.getWindowingStrategyId(strategy);
    String duplicateName =
        components.getWindowingStrategyId(
            WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES));
    assertThat(name, equalTo(duplicateName));
  }
}
