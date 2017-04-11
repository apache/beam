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

package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collections;
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
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.util.WindowingStrategy.AccumulationMode;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
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
  public void registerCoder() throws IOException {
    Coder<?> coder =
        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(SetCoder.of(ByteArrayCoder.of())));
    String id = components.registerCoder(coder);
    assertThat(components.registerCoder(coder), equalTo(id));
    assertThat(id, not(isEmptyOrNullString()));
    VarLongCoder otherCoder = VarLongCoder.of();
    assertThat(components.registerCoder(otherCoder), not(equalTo(id)));

    components.toComponents().getCodersOrThrow(id);
    components.toComponents().getCodersOrThrow(components.registerCoder(otherCoder));
  }

  @Test
  public void registerCoderEqualsNotSame() throws IOException {
    Coder<?> coder =
        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(SetCoder.of(ByteArrayCoder.of())));
    Coder<?> otherCoder =
        KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(SetCoder.of(ByteArrayCoder.of())));
    assertThat(coder, Matchers.<Coder<?>>equalTo(otherCoder));
    String id = components.registerCoder(coder);
    String otherId = components.registerCoder(otherCoder);
    assertThat(otherId, not(equalTo(id)));

    components.toComponents().getCodersOrThrow(id);
    components.toComponents().getCodersOrThrow(otherId);
  }

  @Test
  public void registerTransform() throws IOException {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    String userName = "my_transform/my_nesting";
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            userName, pipeline.begin().expand(), pt.expand(), create, pipeline);
    String componentName = components.registerPTransform(transform);
    assertThat(componentName, equalTo(userName));
    assertThat(components.registerPTransform(transform), equalTo(componentName));
  }

  @Test
  public void registerTransformEmptyFullName() throws IOException {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            "", pipeline.begin().expand(), pt.expand(), create, pipeline);
    String assignedName = components.registerPTransform(transform);

    // A name should be assigned when this PTransform is registered
    assertThat(assignedName, not(isEmptyOrNullString()));

    // However, until its component nodes are specified, that PTransform cannot be a component.
    // Validation should fail
    assertThat(
        components.toComponents().getTransformsOrDefault(assignedName, null), is(nullValue()));
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(assignedName);
    thrown.expectMessage(transform.toString());
    components.validate();
  }

  @Test
  public void registerTransformNullComponents() throws IOException {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    String userName = "my_transform/my_nesting";
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            userName, pipeline.begin().expand(), pt.expand(), create, pipeline);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("child nodes may not be null");
    components.registerPTransform(transform, null);
  }

  @Test
  public void registerTransformNoChildren() throws IOException {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    String userName = "my_transform/my_nesting";
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            userName, pipeline.begin().expand(), pt.expand(), create, pipeline);
    assertThat(components.registerPTransform(transform), not(isEmptyOrNullString()));
    assertThat(
        "Getting a transform ID without providing component transforms "
            + "should not insert a component",
        components.toComponents().getTransformsCount(),
        equalTo(0));
  }

  /**
   * Demonstrates that getting the ID of an {@link AppliedPTransform}, then getting the ID with a
   * list of {@link AppliedPTransform} components returns the same ID, and on the latter call
   * inserts the translation into the components.
   */
  @Test
  public void registerTransformThenRegisterPTransformWithEmptyChildren() throws IOException {
    Create.Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> pt = pipeline.apply(create);
    String userName = "my_transform/my_nesting";
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Integer>, Create.Values<Integer>>of(
            userName, pipeline.begin().expand(), pt.expand(), create, pipeline);
    String originalId = components.registerPTransform(transform);
    assertThat(originalId, not(isEmptyOrNullString()));
    // The original transform should not yet be present in the components, as it hasn't been
    // translated with its children
    assertThat(components.toComponents().getTransformsOrDefault(originalId, null), nullValue());

    String reinserted =
        components.registerPTransform(
            transform, Collections.<AppliedPTransform<?, ?, ?>>emptyList());
    assertThat(reinserted, equalTo(originalId));
    components.toComponents().getTransformsOrThrow(originalId);
  }

  @Test
  public void registerTransformWithUnregisteredChildren() throws IOException {
    Create.Values<Long> create = Create.of(1L, 2L, 3L);
    CountingInput.UnboundedCountingInput createChild = CountingInput.unbounded();

    PCollection<Long> pt = pipeline.apply(create);
    String userName = "my_transform";
    String childUserName = "my_transform/my_nesting";
    AppliedPTransform<?, ?, ?> transform =
        AppliedPTransform.<PBegin, PCollection<Long>, Create.Values<Long>>of(
            userName, pipeline.begin().expand(), pt.expand(), create, pipeline);
    AppliedPTransform<?, ?, ?> childTransform =
        AppliedPTransform.<PBegin, PCollection<Long>, CountingInput.UnboundedCountingInput>of(
            childUserName, pipeline.begin().expand(), pt.expand(), createChild, pipeline);

    String parentId =
        components.registerPTransform(
            transform, Collections.<AppliedPTransform<?, ?, ?>>singletonList(childTransform));
    assertThat(parentId, not(isEmptyOrNullString()));
    components.toComponents().getTransformsOrThrow(parentId);
    // Only the parent should be present within the components
    assertThat(components.toComponents().getTransformsCount(), equalTo(1));
  }

  @Test
  public void registerPCollection() throws IOException {
    PCollection<Long> pCollection = pipeline.apply(CountingInput.unbounded()).setName("foo");
    String id = components.registerPCollection(pCollection);
    assertThat(id, equalTo("foo"));
    components.toComponents().getPcollectionsOrThrow(id);
  }

  @Test
  public void registerPCollectionExistingNameCollision() throws IOException {
    PCollection<Long> pCollection =
        pipeline.apply("FirstCount", CountingInput.unbounded()).setName("foo");
    String firstId = components.registerPCollection(pCollection);
    PCollection<Long> duplicate =
        pipeline.apply("SecondCount", CountingInput.unbounded()).setName("foo");
    String secondId = components.registerPCollection(duplicate);
    assertThat(firstId, equalTo("foo"));
    assertThat(secondId, containsString("foo"));
    assertThat(secondId, not(equalTo("foo")));
    components.toComponents().getPcollectionsOrThrow(firstId);
    components.toComponents().getPcollectionsOrThrow(secondId);
  }

  @Test
  public void registerWindowingStrategy() throws IOException {
    WindowingStrategy<?, ?> strategy =
        WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    String name = components.registerWindowingStrategy(strategy);
    assertThat(name, not(isEmptyOrNullString()));

    components.toComponents().getWindowingStrategiesOrThrow(name);
  }

  @Test
  public void registerWindowingStrategyIdEqualStrategies() throws IOException {
    WindowingStrategy<?, ?> strategy =
        WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES);
    String name = components.registerWindowingStrategy(strategy);
    String duplicateName =
        components.registerWindowingStrategy(
            WindowingStrategy.globalDefault().withMode(AccumulationMode.ACCUMULATING_FIRED_PANES));
    assertThat(name, equalTo(duplicateName));
  }
}
