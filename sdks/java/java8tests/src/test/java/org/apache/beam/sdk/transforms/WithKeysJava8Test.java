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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
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
  public final transient TestPipeline p = TestPipeline.create();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @Category(ValidatesRunner.class)
  public void withLambdaAndTypeDescriptorShouldSucceed() {


    PCollection<String> values = p.apply(Create.of("1234", "3210", "0", "-12"));
    PCollection<KV<Integer, String>> kvs = values.apply(
        WithKeys.of((SerializableFunction<String, Integer>) Integer::valueOf)
                .withKeyType(TypeDescriptor.of(Integer.class)));

    PAssert.that(kvs).containsInAnyOrder(
        KV.of(1234, "1234"), KV.of(0, "0"), KV.of(-12, "-12"), KV.of(3210, "3210"));

    p.run();
  }

  @Test
  public void withLambdaAndNoTypeDescriptorShouldThrow() {

    PCollection<String> values = p.apply(Create.of("1234", "3210", "0", "-12"));

    values.apply("ApplyKeysWithWithKeys", WithKeys.of(Integer::valueOf));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to return a default Coder for ApplyKeysWithWithKeys");

    p.run();
  }
}
