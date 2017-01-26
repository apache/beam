/**
 * Copyright 2016 Seznam a.s.
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
package cz.seznam.euphoria.spark;

import org.apache.spark.SparkConf;

/**
 * Executor running Spark in "local environment". The local execution environment
 * will run the program in a multi-threaded fashion in the same JVM as the
 * environment was created in. Level of parallelism can be optionally
 * set using constructor parameter.
 */
public class TestSparkExecutor extends SparkExecutor {

  public TestSparkExecutor(int parallelism) {
    super(new SparkConf()
            .setMaster("local[" + parallelism + "]")
            .setAppName("test")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer"));
  }

  /**
   * Creates {@link TestSparkExecutor} with default parallelism of 8.
   */
  public TestSparkExecutor() {
    this(8);
  }
}
