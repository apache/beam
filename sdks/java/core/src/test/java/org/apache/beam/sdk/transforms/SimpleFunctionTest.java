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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link SimpleFunction}.
 */
@RunWith(JUnit4.class)
public class SimpleFunctionTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testFailureIfNotOverridden() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("must override");
    thrown.expectMessage("apply");

    new SimpleFunction<Integer, Integer>() {};
  }

  /**
   * Basic test of {@link MapElements} with a lambda (which is instantiated as a {@link
   * SerializableFunction}).
   */
  @Test
  public void testGoodTypeForLambda() throws Exception {
    SimpleFunction<Integer, String> fn = new SimpleFunction<Integer, String>(Object::toString) {};

    assertThat(fn.getInputTypeDescriptor(), equalTo(TypeDescriptors.integers()));
    assertThat(fn.getOutputTypeDescriptor(), equalTo(TypeDescriptors.strings()));
  }

  /**
   * Basic test of {@link MapElements} with a lambda wrapped into a {@link SimpleFunction} to
   * remember its type.
   */
  @Test
  public void testGoodTypeForMethodRef() throws Exception {
    SimpleFunction<Integer, String> fn =
        new SimpleFunction<Integer, String>(SimpleFunctionTest::toStringThisThing) {};

    assertThat(fn.getInputTypeDescriptor(), equalTo(TypeDescriptors.integers()));
    assertThat(fn.getOutputTypeDescriptor(), equalTo(TypeDescriptors.strings()));
  }

  private static String toStringThisThing(Integer i) {
    return i.toString();
  }
}
