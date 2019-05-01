/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.spark.coders;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.TestSparkPipelineOptions;
import org.apache.beam.runners.spark.TestSparkRunner;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/** Testing of beam registrar. */
public class SparkRunnerKryoRegistratorTest {

  @Test
  public void testKryoRegistration() {
    SparkConf conf = new SparkConf();
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.kryo.registrator", WrapperKryoRegistrator.class.getName());
    runSimplePipelineWithSparkContext(conf);
    assertTrue(
        "WrapperKryoRegistrator wasn't initiated, probably KryoSerializer is not set",
        WrapperKryoRegistrator.wasInitiated);
  }

  @Test
  public void testDefaultSerializerNotCallingKryo() {
    SparkConf conf = new SparkConf();
    conf.set("spark.kryo.registrator", KryoRegistratorIsNotCalled.class.getName());
    runSimplePipelineWithSparkContext(conf);
  }

  private void runSimplePipelineWithSparkContext(SparkConf conf) {
    SparkPipelineOptions options =
        PipelineOptionsFactory.create().as(TestSparkPipelineOptions.class);
    options.setRunner(TestSparkRunner.class);

    conf.set("spark.master", "local");
    conf.setAppName("test");

    JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
    options.setUsesProvidedSparkContext(true);
    options.as(SparkContextOptions.class).setProvidedSparkContext(javaSparkContext);
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of("a")); // some operation to trigger pipeline construction
    p.run().waitUntilFinish();
    javaSparkContext.stop();
  }

  /**
   * A {@link SparkRunnerKryoRegistrator} that fails if called. Use only for test purposes. Needs to
   * be public for serialization.
   */
  public static class KryoRegistratorIsNotCalled extends SparkRunnerKryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
      fail(
          "Default spark.serializer is JavaSerializer"
              + " so spark.kryo.registrator shouldn't be called");
    }
  }

  /**
   * A {@link SparkRunnerKryoRegistrator} that registers an internal class to validate
   * KryoSerialization resolution. Use only for test purposes. Needs to be public for serialization.
   */
  public static class WrapperKryoRegistrator extends SparkRunnerKryoRegistrator {

    static boolean wasInitiated = false;

    public WrapperKryoRegistrator() {
      wasInitiated = true;
    }

    @Override
    public void registerClasses(Kryo kryo) {
      super.registerClasses(kryo);
      Registration registration = kryo.getRegistration(MicrobatchSource.class);
      com.esotericsoftware.kryo.Serializer kryoSerializer = registration.getSerializer();
      assertTrue(kryoSerializer instanceof StatelessJavaSerializer);
    }
  }
}
