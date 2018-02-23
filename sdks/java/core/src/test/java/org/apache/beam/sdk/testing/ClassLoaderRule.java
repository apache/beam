/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.testing;

import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * This rule will store the classloader when the test starts
 * and reset it on the contextual thread after the test
 * if used as a test rule (same for the class as a class rule).
 * If you pass it a function to create a classloader it will set it
 * using an {@see InterceptingUrlClassLoader} before the test runs.
 * Finally it handles the reset of the cache of pipeline options factory
 * if set to true before and after the test.
 */
public class ClassLoaderRule implements TestRule, Serializable {
  private final transient Function<ClassLoader, ClassLoader> classLoaderFn;
  private final boolean resetPipelineOptionsCache;

  private ClassLoaderRule(final Function<ClassLoader, ClassLoader> classLoaderFn,
                          final boolean resetPipelineOptionsCache) {
      this.classLoaderFn = classLoaderFn;
      this.resetPipelineOptionsCache = resetPipelineOptionsCache;
  }

  public static Builder build() {
      return new Builder();
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        final Thread thread = Thread.currentThread();
        final ClassLoader loader = thread.getContextClassLoader();
        if (resetPipelineOptionsCache) {
          PipelineOptionsFactory.resetCache();
        }
        final ClassLoader testLoader = classLoaderFn == null ? null : classLoaderFn.apply(loader);
        if (classLoaderFn != null) {
          thread.setContextClassLoader(testLoader);
        }
        try {
          base.evaluate();
        } finally {
          thread.setContextClassLoader(loader);
          if (AutoCloseable.class.isInstance(testLoader)) {
            AutoCloseable.class.cast(testLoader).close();
          }
          if (resetPipelineOptionsCache) {
            PipelineOptionsFactory.resetCache();
          }
        }
      }
    };
  }

  /**
   * The {@see ClassLoaderRule} builder allowing
   * to customize the test classloader if desired and
   * if the pipeline options factory cache should be resetted.
   */
  public static final class Builder {
    private Predicate<String> childClassesFilter;
    private boolean resetPipelineOptionsCache;

    public Builder withPipelineOptionsCacheReset() {
      this.resetPipelineOptionsCache = true;
      return this;
    }

    public Builder withTestChildClassesFilter(final Predicate<String> childClassesFilter) {
      this.childClassesFilter = childClassesFilter;
      return this;
    }

    public ClassLoaderRule create() {
      return new ClassLoaderRule(
        childClassesFilter == null
          ? null : parent -> new InterceptingUrlClassLoader(parent, childClassesFilter),
        resetPipelineOptionsCache);
    }
  }
}
