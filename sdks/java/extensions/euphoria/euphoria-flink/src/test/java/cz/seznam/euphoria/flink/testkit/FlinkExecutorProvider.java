/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.testkit;

import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.flink.FlinkExecutor;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.operator.test.junit.ExecutorEnvironment;
import cz.seznam.euphoria.operator.test.junit.ExecutorProvider;
import org.apache.commons.io.FileUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;

import java.io.File;

public interface FlinkExecutorProvider extends ExecutorProvider {

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
