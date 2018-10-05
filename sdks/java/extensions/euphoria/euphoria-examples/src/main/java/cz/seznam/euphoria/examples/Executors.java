/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
import cz.seznam.euphoria.flink.FlinkExecutor;
import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.inmem.InMemExecutor;
import cz.seznam.euphoria.spark.SparkExecutor;
import cz.seznam.euphoria.spark.TestSparkExecutor;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

import java.io.IOException;

/**
 * A collection of helpers for easy allocation/creation of a specific executor.
 */
public class Executors {

  private interface Factory {
    Executor create() throws IOException;
  }

  private static class InMemFactory implements Factory {
    @Override
    public Executor create() throws IOException {
      return new InMemExecutor();
    }
  }

  private static class SparkFactory implements Factory {
    private final boolean test;
    SparkFactory(boolean test) {
      this.test = test;
    }
    @Override
    public Executor create() {
      if (test) {
        return new TestSparkExecutor();
      } else {
        SparkConf conf = new SparkConf();
        conf.set("spark.serializer", KryoSerializer.class.getName());
        return new SparkExecutor(conf);
      }
    }
  }

  private static class FlinkFactory implements Factory {
    private final boolean test;
    FlinkFactory(boolean test) {
      this.test = test;
    }
    @Override
    public Executor create() throws IOException {
      if (test) {
        return new TestFlinkExecutor();
      } else {
        return new FlinkExecutor();
      }
    }
  }

  /**
   * Creates an executor by name or fails if the specified name is not recognized.
   * Supported names are:
   *
   * <ul>
   *   <li>inmem - the in memory executor (suitable for unit tests)</li>
   *   <li>flink-test - a flink executor for running on the local machine
   *        (suitable for unit tests)</li>
   *   <li>flink - a flink executor capable of running in a distributed fashion</li>
   *   <li>spark-test - a local spark executor for running on the local machine
   *        (suitable for unit tests)</li>
   *   <li>spark - a spark executor capable of running in a distributed fashion</li>
   * </ul>
   *
   * @param executorName the name of the executor to create
   *
   * @return a newly created executor
   *
   * @throws IllegalArgumentException if the specified name is unknown
   * @throws IOException if setting up the executor fails for some reason
   */
  public static Executor createExecutor(String executorName) throws IOException {
    // ~ be sure to go through factories to leverage java lazy class loading;
    // this avoids for example loading spark dependencies in a flink environment
    final Factory f;
    switch (executorName) {
      case "inmem":
        f = new InMemFactory();
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
    return f.create();
  }

}
