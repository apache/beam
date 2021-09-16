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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import com.google.auto.service.AutoService;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for ContextActivationObserverRegistry. */
@RunWith(JUnit4.class)
public class ContextActivationObserverRegistryTest {

  /** This type is used for testing the automatic registration mechanism. */
  private static class AutoRegistrationClass implements ContextActivationObserver {
    @Override
    public void close() throws IOException {}

    @Override
    public Closeable activate(ExecutionStateTracker e) {
      return this;
    }
  }

  /**
   * A {@link ContextActivationObserver.Registrar} to demonstrate default {@link
   * ContextActivationObserver} registration.
   */
  @AutoService(ContextActivationObserver.Registrar.class)
  public static class RegisteredTestContextActivationObserverRegistrar
      implements ContextActivationObserver.Registrar {

    @Override
    public boolean isEnabled() {
      return true;
    }

    @Override
    public ContextActivationObserver getContextActivationObserver() {
      return new AutoRegistrationClass();
    }
  }

  @Test
  public void testContextActivationObserverAll() throws Exception {
    ContextActivationObserverRegistry registry = ContextActivationObserverRegistry.createDefault();
    List<ContextActivationObserver> contextActivationObservers =
        registry.getContextActivationObservers();

    // Check that the registry contains AutoRegistrationClass.
    assertThat(contextActivationObservers, hasItem(instanceOf(AutoRegistrationClass.class)));
  }
}
