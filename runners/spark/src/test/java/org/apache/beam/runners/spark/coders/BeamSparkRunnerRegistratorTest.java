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

import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.Serializer;
import org.junit.Test;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/** Testing of beam registrar. */
public class BeamSparkRunnerRegistratorTest {

  @Test
  public void testKryoRegistration() {

    SparkConf conf = new SparkConf();

    conf.set("spark.master", "local");
    conf.setAppName("test");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    // register immutable collections serializers because the SDK uses them.
    conf.set("spark.kryo.registrator", BeamSparkRunnerRegistrator.class.getName());

    JavaSparkContext sparkContext = new JavaSparkContext(conf);

    ClassTag<ByteArray> byteArrayClassTag = ClassTag$.MODULE$.apply(ByteArray.class);
    Serializer serializer =
        sparkContext.env().serializerManager().getSerializer(byteArrayClassTag, true);
    assertTrue(serializer instanceof KryoSerializer);
  }
}
