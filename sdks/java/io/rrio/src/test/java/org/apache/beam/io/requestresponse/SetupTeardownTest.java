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
package org.apache.beam.io.requestresponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.junit.Rule;
import org.junit.Test;

public class SetupTeardownTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void canSerializeImplementingClasses() {
    SerializableUtils.serializeToByteArray(new SetupTeardownImpl());
  }

  @Test
  public void canSerializeWhenUsedInDoFn() {
    pipeline
        .apply(Create.of(1))
        .apply(ParDo.of(new SetupTeardownUsingDoFn(new SetupTeardownImpl())))
        .setCoder(VarIntCoder.of());

    pipeline.run();
  }

  @Test
  public void canSignalQuotaException() {
    pipeline
        .apply(Create.of(1))
        .apply(ParDo.of(new SetupTeardownUsingDoFn(new ThrowsQuotaException())))
        .setCoder(VarIntCoder.of());

    UncheckedExecutionException exception =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    UserCodeException userCodeException = (UserCodeException) exception.getCause();
    assertEquals(UserCodeQuotaException.class, userCodeException.getCause().getClass());
  }

  @Test
  public void canSignalTimeoutException() {
    pipeline
        .apply(Create.of(1))
        .apply(ParDo.of(new SetupTeardownUsingDoFn(new ThrowsTimeoutException())))
        .setCoder(VarIntCoder.of());

    UncheckedExecutionException exception =
        assertThrows(UncheckedExecutionException.class, pipeline::run);
    UserCodeException userCodeException = (UserCodeException) exception.getCause();
    assertEquals(UserCodeTimeoutException.class, userCodeException.getCause().getClass());
  }

  private static class SetupTeardownUsingDoFn extends DoFn<Integer, Integer> {
    private final SetupTeardown setupTeardown;

    private SetupTeardownUsingDoFn(SetupTeardown setupTeardown) {
      this.setupTeardown = setupTeardown;
    }

    @Setup
    public void setup() throws UserCodeExecutionException {
      setupTeardown.setup();
    }

    @Teardown
    public void teardown() throws UserCodeExecutionException {
      setupTeardown.teardown();
    }

    @ProcessElement
    public void process() {}
  }

  private static class SetupTeardownImpl implements SetupTeardown {
    @Override
    public void setup() throws UserCodeExecutionException {}

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class ThrowsQuotaException implements SetupTeardown {

    @Override
    public void setup() throws UserCodeExecutionException {
      throw new UserCodeQuotaException("quota");
    }

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }

  private static class ThrowsTimeoutException implements SetupTeardown {

    @Override
    public void setup() throws UserCodeExecutionException {
      throw new UserCodeTimeoutException("timeout");
    }

    @Override
    public void teardown() throws UserCodeExecutionException {}
  }
}
