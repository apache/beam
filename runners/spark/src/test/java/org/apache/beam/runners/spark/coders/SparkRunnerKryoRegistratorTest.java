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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Registration;
import org.apache.beam.runners.spark.SparkContextRule;
import org.apache.beam.runners.spark.coders.SparkRunnerKryoRegistratorTest.Others.TestKryoRegistrator;
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
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
@RunWith(Enclosed.class)
public class SparkRunnerKryoRegistratorTest {

  public static class WithKryoSerializer {

    @ClassRule
    public static SparkContextRule contextRule =
        new SparkContextRule(
            KV.of("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            KV.of("spark.kryo.registrator", TestKryoRegistrator.class.getName()));

    @Test
    public void testKryoRegistration() {
      TestKryoRegistrator.wasInitiated = false;
      runSimplePipelineWithSparkContextOptions(contextRule);
      assertTrue(TestKryoRegistrator.wasInitiated);
    }
  }

  public static class WithoutKryoSerializer {
    @ClassRule
    public static SparkContextRule contextRule =
        new SparkContextRule(KV.of("spark.kryo.registrator", TestKryoRegistrator.class.getName()));

    @Test
    public void testDefaultSerializerNotCallingKryo() {
      TestKryoRegistrator.wasInitiated = false;
      runSimplePipelineWithSparkContextOptions(contextRule);
      assertFalse(TestKryoRegistrator.wasInitiated);
    }
  }

  // Hide TestKryoRegistrator from the Enclosed JUnit runner
  interface Others {
    class TestKryoRegistrator extends SparkRunnerKryoRegistrator {

      static boolean wasInitiated = false;

      public TestKryoRegistrator() {
        wasInitiated = true;
      }

      @Override
      public void registerClasses(Kryo kryo) {
        super.registerClasses(kryo);
        // verify serializer for MicrobatchSource
        Registration registration = kryo.getRegistration(MicrobatchSource.class);
        assertTrue(registration.getSerializer() instanceof StatelessJavaSerializer);
      }
    }
  }

  private static void runSimplePipelineWithSparkContextOptions(SparkContextRule context) {
    Pipeline p = Pipeline.create(context.createPipelineOptions());
    p.apply(Create.of("a")); // some operation to trigger pipeline construction
    p.run().waitUntilFinish();
  }
}
