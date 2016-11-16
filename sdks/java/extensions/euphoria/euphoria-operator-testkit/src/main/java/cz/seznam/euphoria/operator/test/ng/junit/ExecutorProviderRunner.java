package cz.seznam.euphoria.operator.test.ng.junit;

import org.junit.internal.builders.JUnit4Builder;
import org.junit.runner.Runner;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

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
                ((AbstractOperatorTest) target).executor =
                    Objects.requireNonNull(env.getExecutor());
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
}
