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
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

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
  public interface TestExecutorService
      extends ExecutorService, BeforeEachCallback, AfterEachCallback {}

  private static class FromSupplier extends ForwardingExecutorService
      implements TestExecutorService {
    private final Supplier<ExecutorService> executorServiceSupplier;
    private ExecutorService delegate;

    private FromSupplier(Supplier<ExecutorService> executorServiceSupplier) {
      this.executorServiceSupplier = executorServiceSupplier;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
      this.delegate = executorServiceSupplier.get();
      if (this.delegate == null) {
        throw new IllegalStateException("The ExecutorService supplier returned null.");
      }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
      Throwable testMethodException = context.getExecutionException().orElse(null);
      Throwable shutdownFailureException = null;

      if (delegate != null) {
        try {
          delegate.shutdown();
          if (!delegate.awaitTermination(5, TimeUnit.SECONDS)) {
            delegate.shutdownNow(); // Attempt to force stop
            // This exception indicates that tasks did not complete cleanly within the timeout.
            shutdownFailureException =
                new IllegalStateException("Test executor failed to shutdown cleanly.");
            // Optionally, one could await a short period after shutdownNow and log if still not
            // terminated,
            // but the primary failure is not shutting down gracefully.
          }
        } catch (InterruptedException ie) {
          // InterruptedException during awaitTermination is a failure mode.
          // Ensure executor is shut down forcefully.
          delegate.shutdownNow();
          Thread.currentThread().interrupt(); // Preserve interrupt status
          shutdownFailureException =
              new IllegalStateException("Test executor shutdown was interrupted.", ie);
        } catch (Exception e) {
          // Catch any other unexpected exceptions during the shutdown sequence
          shutdownFailureException =
              new IllegalStateException("Unexpected exception during test executor shutdown.", e);
        } finally {
          // Ensure delegate is nullified after attempts to shut down,
          // to prevent reuse if the extension instance is somehow reused improperly.
          this.delegate = null;
        }
      }

      if (testMethodException != null) {
        if (shutdownFailureException != null) {
          testMethodException.addSuppressed(shutdownFailureException);
        }
        try {
          throw testMethodException;
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      } else if (shutdownFailureException != null) {
        try {
          throw shutdownFailureException;
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    protected ExecutorService delegate() {
      return delegate;
    }
  }
}
