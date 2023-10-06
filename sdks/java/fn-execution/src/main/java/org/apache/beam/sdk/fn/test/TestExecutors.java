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
package org.apache.beam.sdk.fn.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ForwardingExecutorService;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A {@link TestRule} that validates that all submitted tasks finished and were completed. This
 * allows for testing that tasks have exercised the appropriate shutdown logic.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TestExecutors {
  public static TestExecutorService from(final ExecutorService staticExecutorService) {
    return from(() -> staticExecutorService);
  }

  public static TestExecutorService from(Supplier<ExecutorService> executorServiceSuppler) {
    return new FromSupplier(executorServiceSuppler);
  }

  /** A union of the {@link ExecutorService} and {@link TestRule} interfaces. */
  public interface TestExecutorService extends ExecutorService, TestRule {}

  private static class FromSupplier extends ForwardingExecutorService
      implements TestExecutorService {
    private final Supplier<ExecutorService> executorServiceSupplier;
    private ExecutorService delegate;

    private FromSupplier(Supplier<ExecutorService> executorServiceSupplier) {
      this.executorServiceSupplier = executorServiceSupplier;
    }

    @Override
    public Statement apply(final Statement statement, Description arg1) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          Throwable thrown = null;
          delegate = executorServiceSupplier.get();
          try {
            statement.evaluate();
          } catch (Throwable t) {
            thrown = t;
          }
          shutdown();
          if (!awaitTermination(5, TimeUnit.SECONDS)) {
            shutdownNow();
            IllegalStateException e =
                new IllegalStateException("Test executor failed to shutdown cleanly.");
            if (thrown != null) {
              thrown.addSuppressed(e);
            } else {
              thrown = e;
            }
          }
          if (thrown != null) {
            throw thrown;
          }
        }
      };
    }

    @Override
    protected ExecutorService delegate() {
      return delegate;
    }
  }
}
