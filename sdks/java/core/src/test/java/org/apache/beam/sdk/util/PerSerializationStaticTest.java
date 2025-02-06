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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PerSerializationStaticTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testSharedAcrossDeserialize() throws Exception {
    PerSerializationStatic<AtomicInteger> instance =
        new PerSerializationStatic<>(AtomicInteger::new);
    SerializableUtils.ensureSerializable(instance);

    AtomicInteger i = instance.get();
    i.set(10);
    assertSame(i, instance.get());

    byte[] serialized = SerializableUtils.serializeToByteArray(instance);
    PerSerializationStatic<AtomicInteger> deserialized1 =
        (PerSerializationStatic<AtomicInteger>)
            SerializableUtils.deserializeFromByteArray(serialized, "instance");
    assertSame(i, deserialized1.get());

    PerSerializationStatic<AtomicInteger> deserialized2 =
        (PerSerializationStatic<AtomicInteger>)
            SerializableUtils.deserializeFromByteArray(serialized, "instance");
    assertSame(i, deserialized2.get());
    assertEquals(10, i.get());
  }

  @Test
  public void testDifferentInstancesSeparate() throws Exception {
    PerSerializationStatic<AtomicInteger> instance =
        new PerSerializationStatic<>(AtomicInteger::new);
    SerializableUtils.ensureSerializable(instance);
    AtomicInteger i = instance.get();
    i.set(10);
    assertSame(i, instance.get());

    PerSerializationStatic<AtomicInteger> instance2 =
        new PerSerializationStatic<>(AtomicInteger::new);
    SerializableUtils.ensureSerializable(instance2);
    AtomicInteger j = instance2.get();
    j.set(20);
    assertSame(j, instance2.get());
    assertNotSame(j, i);

    PerSerializationStatic<AtomicInteger> instance1clone = SerializableUtils.clone(instance);
    assertSame(instance1clone.get(), i);
    PerSerializationStatic<AtomicInteger> instance2clone = SerializableUtils.clone(instance2);
    assertSame(instance2clone.get(), j);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDifferentInstancesSeparateNoGetBeforeSerialization() throws Exception {
    PerSerializationStatic<AtomicInteger> instance =
        new PerSerializationStatic<>(AtomicInteger::new);
    SerializableUtils.ensureSerializable(instance);

    PerSerializationStatic<AtomicInteger> instance2 =
        new PerSerializationStatic<>(AtomicInteger::new);
    SerializableUtils.ensureSerializable(instance2);

    byte[] serialized = SerializableUtils.serializeToByteArray(instance);
    PerSerializationStatic<AtomicInteger> deserialized1 =
        (PerSerializationStatic<AtomicInteger>)
            SerializableUtils.deserializeFromByteArray(serialized, "instance");
    PerSerializationStatic<AtomicInteger> deserialized2 =
        (PerSerializationStatic<AtomicInteger>)
            SerializableUtils.deserializeFromByteArray(serialized, "instance");
    assertSame(deserialized1.get(), deserialized2.get());

    PerSerializationStatic<AtomicInteger> instance2clone = SerializableUtils.clone(instance2);
    assertNotSame(instance2clone.get(), deserialized1.get());
  }

  @Test
  public void testDifferentTypes() throws Exception {
    PerSerializationStatic<AtomicInteger> instance =
        new PerSerializationStatic<>(AtomicInteger::new);
    SerializableUtils.ensureSerializable(instance);
    AtomicInteger i = instance.get();
    i.set(10);
    assertSame(i, instance.get());

    PerSerializationStatic<ConcurrentHashMap<Integer, Integer>> instance2 =
        new PerSerializationStatic<>(ConcurrentHashMap::new);
    SerializableUtils.ensureSerializable(instance2);
    ConcurrentHashMap<Integer, Integer> j = instance2.get();
    j.put(1, 100);
    assertSame(j, instance2.get());

    PerSerializationStatic<AtomicInteger> instance1clone = SerializableUtils.clone(instance);
    assertSame(instance1clone.get(), i);
    PerSerializationStatic<ConcurrentHashMap<Integer, Integer>> instance2clone =
        SerializableUtils.clone(instance2);
    assertSame(instance2clone.get(), j);
  }
}
