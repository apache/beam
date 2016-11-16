package cz.seznam.euphoria.operator.test.inmem;

import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import cz.seznam.euphoria.core.executor.inmem.WatermarkTriggerScheduler;
import cz.seznam.euphoria.operator.test.ng.junit.ExecutorEnvironment;
import cz.seznam.euphoria.operator.test.ng.junit.ExecutorProvider;

public interface NgInMemExecutorProvider extends ExecutorProvider {
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
