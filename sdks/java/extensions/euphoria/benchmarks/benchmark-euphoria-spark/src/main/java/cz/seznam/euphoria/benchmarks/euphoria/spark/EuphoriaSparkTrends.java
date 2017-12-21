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
package cz.seznam.euphoria.benchmarks.euphoria.spark;

import com.typesafe.config.Config;
import cz.seznam.euphoria.benchmarks.euphoria.common.trends.EuphoriaTrends;
import cz.seznam.euphoria.benchmarks.euphoria.common.trends.ExecutorFactory;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.spark.SparkExecutor;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class EuphoriaSparkTrends {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: " + EuphoriaSparkTrends.class + " <config-file>");
      System.exit(1);
    }
    new EuphoriaTrends(new File(args[0]), new SparkExecutorFactory()).execute();
  }

  public static class SparkExecutorFactory implements ExecutorFactory {
    @Override
    public Executor newExecutor(Config config, Collection<? extends Class<?>> dataClasses)
        throws IOException {

      SparkConf conf = new SparkConf();
      conf.set("spark.serializer", KryoSerializer.class.getName());
      conf.registerKryoClasses(dataClasses.toArray(new Class[dataClasses.size()]));
      return SparkExecutor
          .newBuilder(EuphoriaSparkTrends.class.getSimpleName(), conf)
          .build();
    }
  }
}
