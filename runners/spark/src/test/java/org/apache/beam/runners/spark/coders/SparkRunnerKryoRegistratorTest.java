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
import org.apache.beam.runners.spark.SparkContextRule;
import org.apache.beam.runners.spark.io.MicrobatchSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

/**
 * Testing of beam registrar. Note: There can only be one Spark context at a time. For that reason
 * tests requiring a different context have to be forked using separate test classes.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
})
@RunWith(Enclosed.class)
public class SparkRunnerKryoRegistratorTest {

  public static class WithKryoSerializer {

    @ClassRule
    public static SparkContextRule contextRule =
        new SparkContextRule(
            KV.of("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            KV.of("spark.kryo.registrator", WrapperKryoRegistrator.class.getName()));

    @Test
    public void testKryoRegistration() {
      runSimplePipelineWithSparkContextOptions(contextRule);
      assertTrue(
          "WrapperKryoRegistrator wasn't initiated, probably KryoSerializer is not set",
          WrapperKryoRegistrator.wasInitiated);
    }

    /**
     * A {@link SparkRunnerKryoRegistrator} that registers an internal class to validate
     * KryoSerialization resolution. Use only for test purposes. Needs to be public for
     * serialization.
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

  public static class WithoutKryoSerializer {
    @ClassRule
    public static SparkContextRule contextRule =
        new SparkContextRule(
            KV.of("spark.kryo.registrator", KryoRegistratorIsNotCalled.class.getName()));

    @Test
    public void testDefaultSerializerNotCallingKryo() {
      runSimplePipelineWithSparkContextOptions(contextRule);
    }

    /**
     * A {@link SparkRunnerKryoRegistrator} that fails if called. Use only for test purposes. Needs
     * to be public for serialization.
     */
    public static class KryoRegistratorIsNotCalled extends SparkRunnerKryoRegistrator {

      @Override
      public void registerClasses(Kryo kryo) {
        fail(
            "Default spark.serializer is JavaSerializer"
                + " so spark.kryo.registrator shouldn't be called");
      }
    }
  }

  private static void runSimplePipelineWithSparkContextOptions(SparkContextRule context) {
    Pipeline p = Pipeline.create(context.createPipelineOptions());
    p.apply(Create.of("a")); // some operation to trigger pipeline construction
    p.run().waitUntilFinish();
  }
}
