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
package org.apache.beam.sdk.testing;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.lang.reflect.Method;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * An extension of {@link TestPipeline} that implements the JUnit5 Extensions model.
 *
 * <p>Usage is the same as {@link TestPipeline}, but the instantiation has to be done via JUnit5's
 * {@link org.junit.jupiter.api.extension.RegisterExtension} annotation mechanism, like
 *
 * <p>The {@link NeedsRunner} category tagging is replaced with JUnit5's {@link
 * org.junit.jupiter.api.Tag} mechanism, the equivalent tag is {@literal needsRunner}.
 *
 * <pre><code>
 * {@literal @RegisterExtension}
 *  public final transient TestPipelineJunit5 p = TestPipelineJunit5.create();
 *
 * {@literal @Test}
 * {@literal @Tag}("needsRunner")
 *  public void myPipelineTest() throws Exception {
 *    final PCollection&lt;String&gt; pCollection = pipeline.apply(...)
 *    PAssert.that(pCollection).containsInAnyOrder(...);
 *    pipeline.run();
 *  }
 * </code></pre>
 *
 * <p>See also the <a href="https://beam.apache.org/contribute/testing/">Testing</a> documentation
 * section.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class TestPipelineJunit5 extends TestPipeline
    implements BeforeEachCallback, AfterEachCallback {

  private TestPipelineJunit5(final PipelineOptions options) {
    super(options);
  }

  public static TestPipelineJunit5 create() {
    return fromOptions(testingPipelineOptions());
  }

  public static TestPipelineJunit5 fromOptions(PipelineOptions options) {
    return new TestPipelineJunit5(options);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    enforcement.get().afterUserCodeFinished();
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    String methodName = context.getTestMethod().map(Method::getName).orElse(null);
    java.util.Optional<Class<?>> testClass = context.getTestClass();

    String appName;
    if (testClass.isPresent() && testClass.get().isMemberClass()) {
      appName =
          String.format(
              "%s$%s-%s",
              testClass.get().getEnclosingClass().getSimpleName(),
              testClass.get().getSimpleName(),
              methodName);
    } else if (testClass.isPresent()) {
      appName = String.format("%s-%s", testClass.get().getSimpleName(), methodName);
    } else {
      appName = String.format("[UNKNOWN CLASS]-%s", methodName);
    }
    getOptions().as(ApplicationNameOptions.class).setAppName(appName);

    if (!enforcement.isPresent()) {
      final boolean annotatedWithNeedsRunner =
          context.getTags().contains("needsRunner")
              || context.getTags().contains("org.apache.beam.sdk.testing.NeedsRunner");

      final boolean crashingRunner =
          CrashingRunner.class.isAssignableFrom(getOptions().getRunner());

      checkState(
          !(annotatedWithNeedsRunner && crashingRunner),
          "The test was annotated with a [@%s] / [@%s] while the runner "
              + "was set to [%s]. Please re-check your configuration.",
          NeedsRunner.class.getSimpleName(),
          ValidatesRunner.class.getSimpleName(),
          getOptions().getRunner().getSimpleName());

      enableAbandonedNodeEnforcement(annotatedWithNeedsRunner || !crashingRunner);
    }
  }
}
