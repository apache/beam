package cz.seznam.euphoria.spark.testkit;

import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.operator.test.junit.ExecutorEnvironment;
import cz.seznam.euphoria.operator.test.junit.ExecutorProvider;
import cz.seznam.euphoria.spark.TestSparkExecutor;

public interface SparkExecutorProvider extends ExecutorProvider {
  @Override
  default ExecutorEnvironment newExecutorEnvironment() throws Exception {
    TestSparkExecutor exec = new TestSparkExecutor();
    return new ExecutorEnvironment() {
      @Override
      public Executor getExecutor() {
        return exec;
      }

      @Override
      public void shutdown() throws Exception {
        exec.shutdown();
      }
    };
  }
}
