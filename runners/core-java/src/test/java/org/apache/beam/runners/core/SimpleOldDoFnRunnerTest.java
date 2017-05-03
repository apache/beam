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
package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.core.BaseExecutionContext.StepContext;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link SimpleOldDoFnRunner} functionality.
 */
@RunWith(JUnit4.class)
public class SimpleOldDoFnRunnerTest {
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
    List<TupleTag<?>> additionalOutputTags = Arrays.asList();
    StepContext context = mock(StepContext.class);
    return new SimpleOldDoFnRunner<>(
        null, fn, null, null, null, additionalOutputTags, context, null);
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
