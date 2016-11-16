package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.operator.test.ng.junit.ExecutorEnvironment;
import cz.seznam.euphoria.operator.test.ng.junit.ExecutorProvider;

public interface NgSparkExecutorProvider extends ExecutorProvider {
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
