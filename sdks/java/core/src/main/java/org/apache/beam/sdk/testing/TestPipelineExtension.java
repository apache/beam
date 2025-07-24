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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline.PipelineAbandonedNodeEnforcement;
import org.apache.beam.sdk.testing.TestPipeline.PipelineRunEnforcement;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * JUnit 5 extension for {@link TestPipeline} that provides the same functionality as the JUnit 4
 * {@link org.junit.rules.TestRule} implementation.
 *
 * <p>Use this extension to test pipelines in JUnit 5:
 *
 * <pre><code>
 * {@literal @}ExtendWith(TestPipelineExtension.class)
 * class MyPipelineTest {
 *   {@literal @}Test
 *   {@literal @}Category(NeedsRunner.class)
 *   void myPipelineTest(TestPipeline pipeline) {
 *     final PCollection&lt;String&gt; pCollection = pipeline.apply(...)
 *     PAssert.that(pCollection).containsInAnyOrder(...);
 *     pipeline.run();
 *   }
 * }
 * </code></pre>
 *
 * <p>The extension will automatically inject a {@link TestPipeline} instance as a parameter to test
 * methods that declare it. It also handles the lifecycle of the pipeline, including enforcement of
 * pipeline execution and abandoned node detection.
 */
public class TestPipelineExtension
    implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(TestPipelineExtension.class);
  private static final String PIPELINE_KEY = "testPipeline";
  private static final String ENFORCEMENT_KEY = "enforcement";

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext) {
    return parameterContext.getParameter().getType() == TestPipeline.class;
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext) {
    return getOrCreateTestPipeline(extensionContext);
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    TestPipeline pipeline = getOrCreateTestPipeline(context);
    Optional<PipelineRunEnforcement> enforcement = getOrCreateEnforcement(context);

    // Set application name based on test method
    String appName = getAppName(context);
    pipeline.getOptions().as(ApplicationNameOptions.class).setAppName(appName);

    // Set up enforcement based on annotations
    setDeducedEnforcementLevel(context, pipeline, enforcement);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    Optional<PipelineRunEnforcement> enforcement = getEnforcement(context);
    if (enforcement.isPresent()) {
      enforcement.get().afterUserCodeFinished();
    }
  }

  private TestPipeline getOrCreateTestPipeline(ExtensionContext context) {
    return context
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(PIPELINE_KEY, key -> TestPipeline.create(), TestPipeline.class);
  }

  private Optional<PipelineRunEnforcement> getOrCreateEnforcement(ExtensionContext context) {
    return context
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            ENFORCEMENT_KEY, key -> Optional.<PipelineRunEnforcement>empty(), Optional.class);
  }

  private Optional<PipelineRunEnforcement> getEnforcement(ExtensionContext context) {
    return context.getStore(NAMESPACE).get(ENFORCEMENT_KEY, Optional.class);
  }

  private void setEnforcement(
      ExtensionContext context, Optional<PipelineRunEnforcement> enforcement) {
    context.getStore(NAMESPACE).put(ENFORCEMENT_KEY, enforcement);
  }

  private String getAppName(ExtensionContext context) {
    String className = context.getTestClass().map(Class::getSimpleName).orElse("UnknownClass");
    String methodName = context.getTestMethod().map(Method::getName).orElse("unknownMethod");
    return className + "-" + methodName;
  }

  private void setDeducedEnforcementLevel(
      ExtensionContext context,
      TestPipeline pipeline,
      Optional<PipelineRunEnforcement> enforcement) {
    // If enforcement level has not been set, do auto-inference
    if (!enforcement.isPresent()) {
      boolean annotatedWithNeedsRunner = hasNeedsRunnerAnnotation(context);

      PipelineOptions options = pipeline.getOptions();
      boolean crashingRunner = CrashingRunner.class.isAssignableFrom(options.getRunner());

      checkState(
          !(annotatedWithNeedsRunner && crashingRunner),
          "The test was annotated with a [@%s] / [@%s] while the runner "
              + "was set to [%s]. Please re-check your configuration.",
          NeedsRunner.class.getSimpleName(),
          ValidatesRunner.class.getSimpleName(),
          CrashingRunner.class.getSimpleName());

      if (annotatedWithNeedsRunner || !crashingRunner) {
        Optional<PipelineRunEnforcement> newEnforcement =
            Optional.of(new PipelineAbandonedNodeEnforcement(pipeline));
        setEnforcement(context, newEnforcement);
      }
    }
  }

  private boolean hasNeedsRunnerAnnotation(ExtensionContext context) {
    // Check method annotations
    Method testMethod = context.getTestMethod().orElse(null);
    if (testMethod != null) {
      if (hasNeedsRunnerCategory(testMethod.getAnnotations())) {
        return true;
      }
    }

    // Check class annotations
    Class<?> testClass = context.getTestClass().orElse(null);
    if (testClass != null) {
      if (hasNeedsRunnerCategory(testClass.getAnnotations())) {
        return true;
      }
    }

    return false;
  }

  private boolean hasNeedsRunnerCategory(Annotation[] annotations) {
    return Arrays.stream(annotations)
        .filter(annotation -> annotation instanceof Category)
        .map(annotation -> (Category) annotation)
        .flatMap(category -> Arrays.stream(category.value()))
        .anyMatch(
            clazz ->
                NeedsRunner.class.isAssignableFrom(clazz)
                    || ValidatesRunner.class.isAssignableFrom(clazz));
  }
}
