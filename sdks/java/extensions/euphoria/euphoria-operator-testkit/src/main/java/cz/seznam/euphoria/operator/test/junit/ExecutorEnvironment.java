package cz.seznam.euphoria.operator.test.junit;

import cz.seznam.euphoria.core.executor.Executor;

public interface ExecutorEnvironment {

  Executor getExecutor();

  void shutdown() throws Exception;

}
