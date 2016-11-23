package cz.seznam.euphoria.flink.testkit;

import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.flink.FlinkExecutor;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.operator.test.ng.junit.ExecutorEnvironment;
import cz.seznam.euphoria.operator.test.ng.junit.ExecutorProvider;
import org.apache.commons.io.FileUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;

import java.io.File;

public interface NgFlinkExecutorProvider extends ExecutorProvider {

  default ExecutorEnvironment newExecutorEnvironment() throws Exception {
    String path = "/tmp/.flink-test-" + System.currentTimeMillis();
    RocksDBStateBackend backend = new RocksDBStateBackend("file://" + path);
    FlinkExecutor executor = new TestFlinkExecutor(ModuloInputSplitAssigner::new).setStateBackend(backend);
    return new ExecutorEnvironment() {
      @Override
      public Executor getExecutor() {
        return executor;
      }
      @Override
      public void shutdown() throws Exception {
        executor.shutdown();
        FileUtils.deleteQuietly(new File(path));
      }
    };
  }
}
