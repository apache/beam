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
package cz.seznam.euphoria.benchmarks.beam;

import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.TestSparkRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;

public class BeamTrendsSpark {

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: Stream <properties-file-path> <exec>");
      System.exit(1);
    }
    
    Parameters params = Parameters.fromFile(args[0]);
    String exec = args[1];
    String[] beamArgs = ArrayUtils.subarray(args, 2, args.length);
    PipelineOptions opts = getOpts(exec, beamArgs);
    BeamTrends.main(opts, params, exec);
  }

  private static PipelineOptions getOpts(String exec, String[] args) {
    switch (exec) {
      case "spark": 
        SparkContextOptions ret1 = PipelineOptionsFactory.fromArgs(args).as(SparkContextOptions.class);
        SparkConf conf1 = getConf(false);
        JavaSparkContext context1 = new JavaSparkContext(conf1);
        ret1.setProvidedSparkContext(context1);
        ret1.setRunner(SparkRunner.class);
        return ret1;
      case "spark-test": 
        SparkConf conf2 = getConf(true);
        JavaSparkContext context2 = new JavaSparkContext(conf2);
        SparkContextOptions ret2 = PipelineOptionsFactory.fromArgs(args).as(SparkContextOptions.class);
        ret2.setProvidedSparkContext(context2);
        ret2.setRunner(TestSparkRunner.class);
        return ret2;
    }
    throw new IllegalArgumentException("Unknown executor: " + exec);
  }

  private static SparkConf getConf(boolean local) {
    SparkConf conf = new SparkConf();
    if (local) {
      conf.setMaster("local[3]");
      conf.setAppName("LocalBeam");
    }
    conf.set("spark.kryo.registrationRequired", "true");
    conf.set("spark.serializer", KryoSerializer.class.getName());
    conf.set("spark.kryo.registrator", BeamSparkRunnerRegistrator.class.getName());
    conf.registerKryoClasses(new Class[]{
        scala.collection.mutable.WrappedArray.ofRef.class,
        Object[].class,
        org.apache.beam.runners.spark.util.ByteArray.class});
    return conf;
  }
}
