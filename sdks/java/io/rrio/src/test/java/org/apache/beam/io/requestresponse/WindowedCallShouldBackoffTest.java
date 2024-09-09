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

import static org.apache.beam.sdk.testing.SerializableMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.beam.sdk.util.SerializableSupplier;
import org.joda.time.Duration;
import org.junit.Test;

/** Tests for {@link WindowedCallShouldBackoff}. */
public class WindowedCallShouldBackoffTest {

  @Test
  public void resetsComputationPerWindow() throws InterruptedException {
    WindowedCallShouldBackoff<Integer> instance = instantiate(Duration.standardSeconds(1L));
    instance.update(new UserCodeExecutionException(""));
    instance.update(new UserCodeExecutionException(""));
    instance.update(new UserCodeExecutionException(""));
    instance.update(new UserCodeExecutionException(""));
    instance.update(new UserCodeExecutionException(""));
    assertThat(instance.isTrue(), equalTo(true));

    Thread.sleep(1001L);
    assertThat(instance.isTrue(), equalTo(false));

    instance.update(new UserCodeExecutionException(""));
    instance.update(new UserCodeExecutionException(""));
    instance.update(new UserCodeExecutionException(""));
    instance.update(new UserCodeExecutionException(""));
    instance.update(new UserCodeExecutionException(""));
    assertThat(instance.isTrue(), equalTo(true));
  }

  private static WindowedCallShouldBackoff<Integer> instantiate(Duration window) {
    return new WindowedCallShouldBackoff<>(
        window,
        (SerializableSupplier<CallShouldBackoff<Integer>>)
            () -> new CallShouldBackoffBasedOnRejectionProbability<Integer>().setThreshold(0.5));
  }
}
