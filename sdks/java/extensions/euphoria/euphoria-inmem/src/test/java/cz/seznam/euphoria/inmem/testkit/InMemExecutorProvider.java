package cz.seznam.euphoria.inmem.testkit;

import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.inmem.InMemExecutor;
import cz.seznam.euphoria.inmem.WatermarkTriggerScheduler;
import cz.seznam.euphoria.operator.test.junit.ExecutorEnvironment;
import cz.seznam.euphoria.operator.test.junit.ExecutorProvider;

public interface InMemExecutorProvider extends ExecutorProvider {
  @Override
  default ExecutorEnvironment newExecutorEnvironment() throws Exception {
    InMemExecutor exec = new InMemExecutor()
        .setTriggeringSchedulerSupplier(() -> new WatermarkTriggerScheduler(500L));
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
