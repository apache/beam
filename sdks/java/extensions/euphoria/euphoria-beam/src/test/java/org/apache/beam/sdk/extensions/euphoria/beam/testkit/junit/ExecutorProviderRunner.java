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
package org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.euphoria.beam.testkit.junit.Processing.Type;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: add javadoc.
 */
public class ExecutorProviderRunner extends Suite {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorProviderRunner.class);

  private final List<Runner> runners = new ArrayList<>();

  public ExecutorProviderRunner(Class<?> klass) throws Throwable {
    super(null, Collections.emptyList());

    // ~ for each encountered test method set up a special runner
    Optional<Type> kPType = getProcessingType(klass);
    ExecutorProvider execProvider = newExecProvider(klass);
    Class<?>[] testClasses = getAnnotatedClasses(klass);
    for (Class<?> testClass : testClasses) {
      boolean isOperatorTest = isAbstractOperatorTest(testClass);

      TestClass tc = new TestClass(testClass);
      List<Object[]> paramsList = getParametersList(tc);
      List<FrameworkMethod> methods = tc.getAnnotatedMethods(Test.class);

      Optional<Type> cPType = getProcessingType(testClass);
      for (FrameworkMethod method : methods) {
        if (isOperatorTest) {
          Optional<Type> mPType = getProcessingType(method.getMethod());
          checkArgument(
              cPType.isPresent() || mPType.isPresent(),
              "Processing annotation is missing either on method or class!");
          Optional<Type> definedPType = merged(cPType, mPType);
          checkArgument(definedPType.isPresent(), "Conflicting processings!");

          Optional<Type> rPType = merged(kPType, definedPType);
          if (rPType.isPresent()) {
            for (Processing.Type ptype : rPType.get().asList()) {
              addRunner(runners, testClass, method, execProvider, ptype, paramsList);
            }
          } else {
            addRunner(runners, testClass, method, execProvider, null, paramsList);
          }
        } else {
          addRunner(runners, testClass, method, execProvider, null, paramsList);
        }
      }
    }
  }

  private static void addRunner(
      List<Runner> acc,
      Class<?> testClass,
      FrameworkMethod method,
      ExecutorProvider execProvider,
      Processing.Type pType,
      List<Object[]> paramsList)
      throws Throwable {
    if (paramsList == null || paramsList.isEmpty()) {
      acc.add(new ExecutorProviderTestMethodRunner(testClass, method, execProvider, pType, null));
    } else {
      for (Object[] params : paramsList) {
        acc.add(
            new ExecutorProviderTestMethodRunner(testClass, method, execProvider, pType, params));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Object[]> getParametersList(TestClass klass) throws Throwable {
    FrameworkMethod parametersMethod = getParametersMethod(klass);
    if (parametersMethod == null) {
      return null;
    }
    return (List<Object[]>) parametersMethod.invokeExplosively(null);
  }

  private static FrameworkMethod getParametersMethod(TestClass testClass) throws Exception {
    List<FrameworkMethod> methods = testClass.getAnnotatedMethods(Parameterized.Parameters.class);
    if (methods.isEmpty()) {
      return null;
    }

    for (FrameworkMethod each : methods) {
      int modifiers = each.getMethod().getModifiers();
      if (Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers)) {
        return each;
      }
    }
    throw new Exception("No public static parameters method on class " + testClass.getName());
  }

  private static Class<?>[] getAnnotatedClasses(Class<?> klass) throws InitializationError {

    SuiteClasses annotation = klass.getAnnotation(SuiteClasses.class);
    if (annotation == null) {
      return new Class[] {klass};
    }
    return annotation.value();
  }

  private static ExecutorProvider newExecProvider(Class<?> klass) throws InitializationError {

    if (!ExecutorProvider.class.isAssignableFrom(klass)) {
      throw new IllegalArgumentException(
          "Annotated class must implement " + ExecutorProvider.class);
    }

    try {
      return ExecutorProvider.class.cast(klass.newInstance());
    } catch (IllegalAccessException e) {
      throw new InstantiationError(
          "Default constructor of " + klass + " must be public: " + e.getMessage());
    } catch (InstantiationException e) {
      throw new InstantiationError("Failed to initialize " + klass + ": " + e);
    }
  }

  // return defined processing type (bounded, unbounded, any) from annotation
  private static Optional<Processing.Type> getProcessingType(AnnotatedElement element) {
    if (element.isAnnotationPresent(Processing.class)) {
      Processing proc = (Processing) element.getAnnotation(Processing.class);
      return Optional.of(proc.value());
    } else {
      return Optional.empty();
    }
  }

  // merges the given processings. Optional.empty represents undefined
  private static Optional<Type> merged(Optional<Type> x, Optional<Type> y) {
    return Stream.of(x, y)
        .filter(Optional::isPresent)
        .reduce((acc, next) -> acc.flatMap(a -> a.merge(next.get())))
        .orElse(Optional.empty());
  }

  static boolean isAbstractOperatorTest(Class<?> klass) {
    return AbstractOperatorTest.class.isAssignableFrom(klass);
  }

  @Override
  protected List<Runner> getChildren() {
    return runners;
  }

  static class ExecutorProviderTestMethodRunner extends BlockJUnit4ClassRunner {
    private final ExecutorProvider execProvider;
    private final Processing.Type procType;
    private final FrameworkMethod method;
    private final Object[] parameterList;

    ExecutorProviderTestMethodRunner(
        Class<?> testClass,
        FrameworkMethod method,
        ExecutorProvider execProvider,
        Processing.Type ptype,
        Object[] parameterList)
        throws InitializationError {
      super(testClass);
      this.execProvider = execProvider;
      this.procType = ptype;
      this.method = method;
      this.parameterList = parameterList;
    }

    @Override
    protected void validateConstructor(List<Throwable> errors) {
      validateOnlyOneConstructor(errors);
    }

    @Override
    protected List<FrameworkMethod> getChildren() {
      return Collections.singletonList(method);
    }

    @Override
    protected String testName(FrameworkMethod method) {
      StringBuilder buf = new StringBuilder();
      buf.append(super.testName(method));
      if (parameterList != null && parameterList.length > 0) {
        buf.append("(");
        for (int i = 0; i < parameterList.length; i++) {
          if (i > 0) {
            buf.append(", ");
          }
          buf.append(parameterList[i]);
        }
        buf.append(")");
      }
      if (isAbstractOperatorTest()) {
        buf.append("[").append(procType == null ? "UNDEFINED" : procType).append("]");
      }
      return buf.toString();
    }

    @Override
    protected Object createTest() throws Exception {
      if (parameterList == null || parameterList.length == 0) {
        return getTestClass().getOnlyConstructor().newInstance();
      } else {
        return getTestClass().getOnlyConstructor().newInstance(parameterList);
      }
    }

    private boolean isAbstractOperatorTest() {
      return ExecutorProviderRunner.isAbstractOperatorTest(getTestClass().getJavaClass());
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {
      Description description = describeChild(method);
      if (method.getAnnotation(Ignore.class) != null) {
        notifier.fireTestIgnored(description);
      } else {
        if (isAbstractOperatorTest() && procType == null) {
          notifier.fireTestIgnored(description);
        } else {
          runLeaf(methodBlock(method), description, notifier);
        }
      }
    }

    @Override
    protected Statement withAfters(FrameworkMethod method, Object target, Statement statement) {

      Statement result = super.withAfters(method, target, statement);
      if (target instanceof AbstractOperatorTest) {
        return new Statement() {
          @Override
          public void evaluate() throws Throwable {
            ExecutorEnvironment env = execProvider.newExecutorEnvironment();

            AbstractOperatorTest opTest = ((AbstractOperatorTest) target);
            opTest.executor = Objects.requireNonNull(env.getExecutor());
            opTest.processing = procType;
            try {
              result.evaluate();
            } finally {
              try {
                env.shutdown();
              } catch (RuntimeException e) {
                LOG.debug("Failed to cleanly shut down executor environment.", e);
              }
            }
          }
        };
      }
      return result;
    }
  }
}
