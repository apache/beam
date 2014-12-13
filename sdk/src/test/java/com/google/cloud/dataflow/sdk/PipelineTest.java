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

package com.google.cloud.dataflow.sdk;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.util.UserCodeException;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for Pipeline.
 */
@RunWith(JUnit4.class)
public class PipelineTest {

  static class PipelineWrapper extends Pipeline {
    protected PipelineWrapper(PipelineRunner<?> runner) {
      super(runner, PipelineOptionsFactory.create());
    }
  }

  // Mock class that throws a user code exception during the call to
  // Pipeline.run().
  static class TestPipelineRunnerThrowingUserException
      extends PipelineRunner<PipelineResult> {
    @Override
    public PipelineResult run(Pipeline pipeline) {
      Throwable t = new IllegalStateException("user code exception");
      throw new UserCodeException(t);
    }
  }

  // Mock class that throws an SDK or API client code exception during
  // the call to Pipeline.run().
  static class TestPipelineRunnerThrowingSDKException
      extends PipelineRunner<PipelineResult> {
    @Override
    public PipelineResult run(Pipeline pipeline) {
      throw new IllegalStateException("SDK exception");
    }
  }

  @Test
  public void testPipelineUserExceptionHandling() {
    Pipeline p = new PipelineWrapper(
        new TestPipelineRunnerThrowingUserException());

    // Check pipeline runner correctly catches user errors.
    try {
      Object results = p.run();
      fail("Should have thrown an exception.");
    } catch (RuntimeException exn) {
      // Make sure users don't have to worry about the
      // UserCodeException wrapper.
      Assert.assertThat(exn, not(instanceOf(UserCodeException.class)));
      // Assert that the message is correct.
      Assert.assertThat(
          exn.getMessage(), containsString("user code exception"));
      // Cause should be IllegalStateException.
      Assert.assertThat(
          exn.getCause(), instanceOf(IllegalStateException.class));
    }
  }

  @Test
  public void testPipelineSDKExceptionHandling() {
    Pipeline p = new PipelineWrapper(new TestPipelineRunnerThrowingSDKException());

    // Check pipeline runner correctly catches SDK errors.
    try {
      Object results = p.run();
      fail("Should have thrown an exception.");
    } catch (RuntimeException exn) {
      // Make sure the exception isn't a UserCodeException.
      Assert.assertThat(exn, not(instanceOf(UserCodeException.class)));
      // Assert that the message is correct.
      Assert.assertThat(exn.getMessage(), containsString("SDK exception"));
      // RuntimeException should be IllegalStateException.
      Assert.assertThat(exn, instanceOf(IllegalStateException.class));
    }
  }
}
