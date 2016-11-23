package cz.seznam.euphoria.operator.test.ng.junit;

import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import cz.seznam.euphoria.operator.test.ng.junit.Processing.Type;
import org.junit.internal.builders.JUnit4Builder;
import org.junit.runner.Runner;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public class ExecutorProviderRunner extends Suite {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorProviderRunner.class);

  public ExecutorProviderRunner(Class<?> klass) throws InitializationError {
    super(new ExecutorProviderRunnerBuilder(
        newExecProvider(klass)), getAnnotatedClasses(klass));
  }

  private static Class<?>[] getAnnotatedClasses(Class<?> klass) throws InitializationError {
    SuiteClasses annotation = klass.getAnnotation(SuiteClasses.class);
    if (annotation == null) {
      return new Class[]{klass};
    }
    return annotation.value();
  }

  static ExecutorProvider newExecProvider(Class<?> klass) throws InitializationError {
    if (!ExecutorProvider.class.isAssignableFrom(klass)) {
      throw new IllegalArgumentException("Annotated class must implement " + ExecutorProvider.class);
    }

    try {
      return ExecutorProvider.class.cast(klass.newInstance());
    } catch (IllegalAccessException e) {
      throw new InstantiationError("Default constructor of "
          + klass + " must be public: " + e.getMessage());
    } catch (InstantiationException e) {
      throw new InstantiationError("Failed to initialize " + klass + ": " + e);
    }
  }

  static class ExecutorProviderRunnerBuilder extends JUnit4Builder {
    final ExecutorProvider provider;

    ExecutorProviderRunnerBuilder(ExecutorProvider provider) {
      this.provider = Objects.requireNonNull(provider);
    }

    @Override
    public Runner runnerForClass(Class<?> testClass) throws Throwable {
      return new BlockJUnit4ClassRunner(testClass) {
        @Override
        protected Statement withAfters(FrameworkMethod method,
                                       Object target,
                                       Statement statement) {

          Statement result = super.withAfters(method, target, statement);
          if (target instanceof AbstractOperatorTest) {
            return new Statement() {
              @Override
              public void evaluate() throws Throwable {
                ExecutorEnvironment env = provider.newExecutorEnvironment();
                AbstractOperatorTest opTest = ((AbstractOperatorTest) target);
                opTest.executor = Objects.requireNonNull(env.getExecutor());
                // annotation must be present on test class
                Optional<Processing.Type> testClassProcessing = getProcessing(testClass);
                // annotation may be present on execution method
                Optional<Processing.Type> testMethodProcessing = getProcessing(method.getMethod());
                Preconditions.checkArgument(
                    testClassProcessing.isPresent() || testMethodProcessing.isPresent(),
                    "Processing annotation is missing either on method or class!");
                Optional<Processing.Type> definedProcessing = merged(testClassProcessing, testMethodProcessing);
                Preconditions.checkArgument(definedProcessing.isPresent(), "Conflicting processings!");
                // annotation may be present on execution class which can override the defined
                Optional<Processing.Type> runnerProcessing = getProcessing(provider.getClass());
                // merge processing types if both defined
                opTest.processing = merged(definedProcessing, runnerProcessing).orElse(null);
                LOG.info("Defined processing for {} is {}", 
                         method.getMethod().getName(), opTest.processing);
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
      };
    }
  }

  // return defined processing type (bounded, unbounded, any) from annotation
  private static Optional<Processing.Type> getProcessing(AnnotatedElement element) {
    if (element.isAnnotationPresent(Processing.class)) {
      Processing proc = (Processing) element.getAnnotation(Processing.class);
      return Optional.of(proc.value());
    } else {
      return Optional.empty();
    }
  }
  
  // merges all processings. Optional.empty represents undefined
  private static Optional<Type> merged(Optional<Type> ... processings) {
    return Arrays.asList(processings).stream()
        .filter(Optional::isPresent)
        .reduce((acc, next) -> acc.flatMap(a -> a.merge(next.get())))
        .get();
  }
}
