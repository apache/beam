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
package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Java 8 Tests for {@link WithKeys}.
 */
@RunWith(JUnit4.class)
public class WithKeysJava8Test {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(RunnableOnService.class)
  public void withLambdaAndTypeDescriptorShouldSucceed() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> values = p.apply(Create.of("1234", "3210", "0", "-12"));
    PCollection<KV<Integer, String>> kvs = values.apply(
        WithKeys.of((String s) -> Integer.valueOf(s))
                .withKeyType(TypeDescriptor.of(Integer.class)));

    DataflowAssert.that(kvs).containsInAnyOrder(
        KV.of(1234, "1234"), KV.of(0, "0"), KV.of(-12, "-12"), KV.of(3210, "3210"));

    p.run();
  }

  @Test
  public void withLambdaAndNoTypeDescriptorShouldThrow() {
    TestPipeline p = TestPipeline.create();

    PCollection<String> values = p.apply(Create.of("1234", "3210", "0", "-12"));

    values.apply("ApplyKeysWithWithKeys", WithKeys.of((String s) -> Integer.valueOf(s)));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectMessage("Unable to return a default Coder for ApplyKeysWithWithKeys");
    thrown.expectMessage("Cannot provide a coder for type variable K");
    thrown.expectMessage("the actual type is unknown due to erasure.");

    p.run();
  }
}

