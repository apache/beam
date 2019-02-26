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
package org.apache.beam.runners.flink.translation.functions;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link ImpulseSourceFunction}. */
public class ImpulseSourceFunctionTest {

  private static final Logger LOG = LoggerFactory.getLogger(ImpulseSourceFunctionTest.class);

  @Rule public TestName testName = new TestName();

  private final SourceFunction.SourceContext<WindowedValue<byte[]>> sourceContext;
  private final ImpulseElementMatcher elementMatcher = new ImpulseElementMatcher();

  public ImpulseSourceFunctionTest() {
    this.sourceContext = Mockito.mock(SourceFunction.SourceContext.class);
  }

  @Test
  public void testInstanceOfSourceFunction() {
    // should be a non-parallel source function
    assertThat(new ImpulseSourceFunction(false), instanceOf(SourceFunction.class));
  }

  @Test(timeout = 10_000)
  public void testImpulse() throws Exception {
    ImpulseSourceFunction source = new ImpulseSourceFunction(false);
    source.run(sourceContext);
    // should finish
    verify(sourceContext).collect(argThat(elementMatcher));
  }

  @Test(timeout = 10_000)
  public void testKeepAlive() throws Exception {
    ImpulseSourceFunction source = new ImpulseSourceFunction(true);
    Thread sourceThread =
        new Thread(
            () -> {
              try {
                source.run(sourceContext);
                // should not finish
              } catch (Exception e) {
                LOG.error("Exception while executing ImpulseSourceFunction", e);
              }
            });
    try {
      sourceThread.start();
      source.cancel();
      // should finish
      sourceThread.join();
    } finally {
      sourceThread.interrupt();
      sourceThread.join();
    }
    verify(sourceContext).collect(argThat(elementMatcher));
  }

  @Test(timeout = 10_000)
  public void testKeepAliveDuringInterrupt() throws Exception {
    ImpulseSourceFunction source = new ImpulseSourceFunction(true);
    Thread sourceThread =
        new Thread(
            () -> {
              try {
                source.run(sourceContext);
                // should not finish
              } catch (Exception e) {
                LOG.error("Exception while executing ImpulseSourceFunction", e);
              }
            });

    sourceThread.start();
    sourceThread.interrupt();
    Thread.sleep(200);
    assertThat(sourceThread.isAlive(), is(true));
    // should quit
    source.cancel();
    sourceThread.interrupt();
    sourceThread.join();
    verify(sourceContext).collect(argThat(elementMatcher));
  }

  private static class ImpulseElementMatcher extends ArgumentMatcher<WindowedValue<byte[]>> {

    @Override
    public boolean matches(Object o) {
      return o instanceof WindowedValue
          && Arrays.equals((byte[]) ((WindowedValue) o).getValue(), new byte[] {});
    }
  }
}
