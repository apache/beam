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

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.BaseExecutionContext.StepContext;
import org.apache.beam.sdk.values.TupleTag;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for base {@link DoFnRunnerBase} functionality.
 */
@RunWith(JUnit4.class)
public class SimpleDoFnRunnerTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testExceptionsWrappedAsUserCodeException() {
    ThrowingDoFn fn = new ThrowingDoFn();
    DoFnRunner<String, String> runner = createRunner(fn);

    thrown.expect(UserCodeException.class);
    thrown.expectCause(is(fn.exceptionToThrow));

    runner.processElement(WindowedValue.valueInGlobalWindow("anyValue"));
  }

  @Test
  public void testSystemDoFnInternalExceptionsNotWrapped() {
    ThrowingSystemDoFn fn = new ThrowingSystemDoFn();
    DoFnRunner<String, String> runner = createRunner(fn);

    thrown.expect(is(fn.exceptionToThrow));

    runner.processElement(WindowedValue.valueInGlobalWindow("anyValue"));
  }

  private DoFnRunner<String, String> createRunner(OldDoFn<String, String> fn) {
    // Pass in only necessary parameters for the test
    List<TupleTag<?>> sideOutputTags = Arrays.asList();
    StepContext context = mock(StepContext.class);
    return new SimpleDoFnRunner<>(
          null, fn, null, null, null, sideOutputTags, context, null, null);
  }

  static class ThrowingDoFn extends OldDoFn<String, String> {
    final Exception exceptionToThrow =
        new UnsupportedOperationException("Expected exception");

    @Override
    public void processElement(ProcessContext c) throws Exception {
      throw exceptionToThrow;
    }
  }

  @SystemDoFnInternal
  static class ThrowingSystemDoFn extends ThrowingDoFn {
  }
}
