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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Source;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;


/**
 * BeamSparkRunnerRegistrator Test.
 */
public class BeamSparkRunnerRegistratorTest {
  @Test
  public void testCodersAndSourcesRegistration() {
    BeamSparkRunnerRegistrator registrator = new BeamSparkRunnerRegistrator();

    Reflections reflections = new Reflections();
    Iterable<Class<? extends Serializable>> classesForJavaSerialization =
        Iterables.concat(reflections.getSubTypesOf(Coder.class),
            reflections.getSubTypesOf(Source.class));

    Kryo kryo = new Kryo();

    registrator.registerClasses(kryo);

    for (Class<?> clazz : classesForJavaSerialization) {
      Assert.assertThat("Registered serializer for class " + clazz.getName()
              + " was not an instance of " + JavaSerializer.class.getName(),
          kryo.getSerializer(clazz),
          Matchers.instanceOf(JavaSerializer.class));
    }
  }
}
