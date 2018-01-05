/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.testing.junit5.internal;

import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipelineHandler;
import org.apache.beam.sdk.testing.junit5.BeamInject;
import org.apache.beam.sdk.testing.junit5.WithApacheBeam;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;

/**
 * JUnit 5 integration code glue.
 */
public class BeamJUnit5Extension implements
        BeforeAllCallback, AfterAllCallback,
        BeforeEachCallback, AfterEachCallback,
        TestInstancePostProcessor, ParameterResolver,
        ExecutionCondition {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
            .create(BeamJUnit5Extension.class.getName());

    private static final String RELEASE_PHASE = BeamJUnit5Extension.class.getName() + "#release";

    @Override
    public void beforeAll(final ExtensionContext context) {
        final ExtensionContext.Store store = context.getStore(NAMESPACE);
        store.put(RELEASE_PHASE, "all");
    }

    @Override
    public void afterAll(final ExtensionContext context) throws Exception {
        final Object closeable = context.getStore(NAMESPACE).get(AutoCloseable.class.getName());
        if (closeable != null) {
            AutoCloseable.class.cast(closeable).close();
        }
    }

    @Override
    public void beforeEach(final ExtensionContext context) {
        final ExtensionContext.Store store = context.getStore(NAMESPACE);
        if (store.get(AutoCloseable.class.getName()) == null) {
            store.put(AutoCloseable.class.getName(), getOrCreateHandler(context).start(context));
        }
    }

    @Override
    public void afterEach(final ExtensionContext context) throws Exception {
        if (context.getStore(NAMESPACE).get(RELEASE_PHASE) == null) {
            afterAll(context);
        }
    }

    @Override
    public boolean supportsParameter(final ParameterContext parameterContext,
                                     final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return TestPipelineHandler.class.isAssignableFrom(
                parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(final ParameterContext parameterContext,
                                   final ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return getOrCreateHandler(extensionContext);
    }

    @Override
    public void postProcessTestInstance(final Object testInstance,
                                        final ExtensionContext context) {
        Class<?> testClass = context.getRequiredTestClass();
        while (testClass != Object.class) {
            Stream.of(testClass.getDeclaredFields())
                  .filter(c -> c.isAnnotationPresent(BeamInject.class))
                  .forEach(f -> {
                    if (!TestPipelineHandler.class.isAssignableFrom(f.getType())) {
                        throw new IllegalArgumentException("@BeamInject not supported on " + f);
                    }
                    if (!f.isAccessible()) {
                        f.setAccessible(true);
                    }
                    try {
                        f.set(testInstance, getOrCreateHandler(context));
                    } catch (final IllegalAccessException e) {
                        throw new IllegalStateException(e);
                    }
                });
            testClass = testClass.getSuperclass();
        }
    }

    @Override // responsible to deactivate methods requiring a runner if it is not set
    public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
        if (!context.getTestMethod().isPresent()) {
            return ConditionEvaluationResult.enabled("Classes are always active");
        }
        try {
            if (doesNeedsRunner(context) && skipOnMissingRunner(context)
                    && CrashingRunner.class.isAssignableFrom(TestPipelineHandler.class.cast(
                            getOrCreateHandler(context)).getOptions().getRunner())) {
                return ConditionEvaluationResult.disabled("No available runner");
            }
        } catch (final IllegalArgumentException iae) {
            return ConditionEvaluationResult.disabled("No available runner: " + iae.getMessage());
        }
        return ConditionEvaluationResult.enabled("Test active");
    }

    private boolean skipOnMissingRunner(final ExtensionContext context) {
        final Optional<WithApacheBeam> config = context.getElement()
                .map(e -> Optional.ofNullable(e.getAnnotation(WithApacheBeam.class)))
                .filter(Optional::isPresent)
                .orElseGet(() -> context.getTestClass()
                        .map(e -> e.getAnnotation(WithApacheBeam.class)));
        return config.map(WithApacheBeam::skipMissingRunnerTests).orElse(true);
    }

    // we support the tag "NeedsRunner" or fqn categories (interfaces)
    private boolean doesNeedsRunner(final ExtensionContext context) {
        return context.getTags().stream().anyMatch(tag -> {
            if (NeedsRunner.class.getSimpleName().equalsIgnoreCase(tag)) {
                return true;
            }
            try { // compat with junit 4 mode but weird in junit 5
                return NeedsRunner.class.isAssignableFrom(
                        BeamJUnit5Extension.class.getClassLoader()
                                .loadClass(tag.trim()));
            } catch (final Exception e) {
                return false;
            }
        });
    }

    private TestPipelineHandler<ExtensionContext> getOrCreateHandler(
            final ExtensionContext context) {
        return Optional.ofNullable(TestPipelineHandler.class.cast(context.getStore(NAMESPACE)
                    .get(TestPipelineHandler.class.getName())))
                .orElseGet(() -> {
                    final TestPipelineHandler<ExtensionContext> testPipeline =
                            new TestPipelineHandler<ExtensionContext>(findOptions(context)) {

                                @Override
                                protected boolean doesNeedRunner(final ExtensionContext context) {
                                    return BeamJUnit5Extension.this
                                            .doesNeedsRunner(context);
                                }

                                @Override
                                protected String getAppName(final ExtensionContext context) {
                                    final String methodName = context
                                            .getRequiredTestMethod()
                                            .getName();
                                    final Class<?> testClass = context
                                            .getRequiredTestClass();
                                    return appName(testClass, methodName);
                                }
                            };

                    context.getElement()
                            .flatMap(e -> Optional.ofNullable(
                                    e.getAnnotation(WithApacheBeam.class)))
                            .ifPresent(config -> {
                                testPipeline.enableAbandonedNodeEnforcement(
                                        config.enableAbandonedNodeEnforcement());
                                testPipeline.enableAutoRunIfMissing(
                                        config.enableAutoRunIfMissing());
                            });

                    context.getStore(NAMESPACE)
                            .put(TestPipelineHandler.class.getName(), testPipeline);
                    return testPipeline;
                });
    }

    // options are deduced from the @WithApacheBeam config,
    // if empty it uses the default logic with the system properties
    private PipelineOptions findOptions(final ExtensionContext context) {
        return context.getElement()
                .flatMap(e -> Optional.ofNullable(e.getAnnotation(WithApacheBeam.class))
                                      .map(WithApacheBeam::options))
                .map(PipelineOptionsFactory::fromArgs).map(PipelineOptionsFactory.Builder::create)
                .orElseGet(TestPipelineHandler::testingPipelineOptions);
    }
}
