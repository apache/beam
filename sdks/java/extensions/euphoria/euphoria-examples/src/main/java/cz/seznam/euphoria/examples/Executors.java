/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.examples;

import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.executor.local.LocalExecutor;
import cz.seznam.euphoria.flink.FlinkExecutor;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.spark.SparkExecutor;

import java.io.IOException;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;

/**
 * A collection of helpers for easy allocation/creation of a specific executor.
 */
public class Executors {

  private interface Factory {
    Executor create(Class<?>... classes) throws IOException;
  }

  private static class LocalFactory implements Factory {

    @Override
    public Executor create(Class<?>... classes) throws IOException {
      return new LocalExecutor();
    }
  }

  private static class SparkFactory implements Factory {

    private final boolean test;

    SparkFactory(boolean test) {
      this.test = test;
    }

    @Override
    public Executor create(Class<?>... classes) {
      final SparkExecutor.Builder builder = SparkExecutor
          .newBuilder("euphoria-example")
          .registerKryoClasses(classes);
      if (test) {
        return builder.local().build();
      } else {
        return builder.build();
      }
    }
  }

  private static class FlinkFactory implements Factory {

    private final boolean test;

    FlinkFactory(boolean test) {
      this.test = test;
    }

    @Override
    public Executor create(Class<?>... classes) throws IOException {
      if (test) {
        return new TestFlinkExecutor();
      } else {
        return new FlinkExecutor()
            .setStateBackend(new RocksDBStateBackend("hdfs:///tmp/flink/checkpoints"))
            .registerClasses(classes);
      }
    }
  }

  /**
   * Creates an executor by name or fails if the specified name is not recognized.
   * Supported names are:
   *
   * <ul>
   *   <li>local - the local executor (suitable for unit tests)</li>
   *   <li>flink-test - a flink executor for running on the local machine
   *        (suitable for unit tests)</li>
   *   <li>flink - a flink executor capable of running in a distributed fashion</li>
   *   <li>spark-test - a local spark executor for running on the local machine
   *        (suitable for unit tests)</li>
   *   <li>spark - a spark executor capable of running in a distributed fashion</li>
   * </ul>
   *
   * @param executorName the name of the executor to create
   * @param classes classes used by the flow to be registered for serialization
   *
   * @return a newly created executor
   *
   * @throws IllegalArgumentException if the specified name is unknown
   * @throws IOException if setting up the executor fails for some reason
   */
  public static Executor createExecutor(
      String executorName,
      Class<?>... classes) throws IOException {

    // ~ be sure to go through factories to leverage java lazy class loading;
    // this avoids for example loading spark dependencies in a flink environment
    final Factory f;
    switch (executorName) {
      case "local":
        f = new LocalFactory();
        break;
      case "flink-test":
        f = new FlinkFactory(true);
        break;
      case "flink":
        f = new FlinkFactory(false);
        break;
      case "spark-test":
        f = new SparkFactory(true);
        break;
      case "spark":
        f = new SparkFactory(false);
        break;
      default:
        throw new IllegalArgumentException("Executor not supported: " + executorName);
    }
    return f.create(classes);
  }

}
