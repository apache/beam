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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExpiringMemoizingSerializableSupplierTest {

  @Test
  public void testSupplierIsSerializable() {
    final ExpiringMemoizingSerializableSupplier<?> instance =
        new ExpiringMemoizingSerializableSupplier<>(
            Object::new, Duration.ZERO, null, Duration.ZERO);

    // Instances must be serializable.
    SerializableUtils.ensureSerializable(instance);
  }

  @Test
  public void testSameValueAfterConstruction() {
    final Object initialValue = new Object();
    final ExpiringMemoizingSerializableSupplier<Object> instance =
        new ExpiringMemoizingSerializableSupplier<>(
            Object::new, Duration.ofHours(1), initialValue, Duration.ofHours(1));

    // Construction initializes deadlineNanos for delayed expiration.
    // The supplied value must not be observed as uninitialized
    // The supplied value is referentially equal to initialValue.
    final Object instanceValue = instance.get();
    assertNotNull(instanceValue);
    assertSame(initialValue, instanceValue);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDistinctValuesAfterDeserialization() throws Exception {
    final Object initialValue = new Object();
    final ExpiringMemoizingSerializableSupplier<Object> instance =
        new ExpiringMemoizingSerializableSupplier<>(
            Object::new, Duration.ofHours(1), initialValue, Duration.ofHours(1));

    // Deserialized instances must be referentially distinct for the purpose of this test.
    final byte[] serialized = SerializableUtils.serializeToByteArray(instance);
    final ExpiringMemoizingSerializableSupplier<Object> deserialized1 =
        (ExpiringMemoizingSerializableSupplier<Object>)
            SerializableUtils.deserializeFromByteArray(serialized, "instance");
    final ExpiringMemoizingSerializableSupplier<Object> deserialized2 =
        (ExpiringMemoizingSerializableSupplier<Object>)
            SerializableUtils.deserializeFromByteArray(serialized, "instance");
    assertNotSame(instance, deserialized1);
    assertNotSame(instance, deserialized2);
    assertNotSame(deserialized1, deserialized2);

    // Deserialization initializes deadlineNanos for immediate expiration.
    // Supplied values must not be observed as uninitialized.
    // The initial and supplied values are all referentially distinct.
    final Object deserialized1Value = deserialized1.get();
    final Object deserialized2Value = deserialized2.get();
    assertNotNull(deserialized1Value);
    assertNotNull(deserialized2Value);
    assertNotSame(initialValue, deserialized1Value);
    assertNotSame(initialValue, deserialized2Value);
    assertNotSame(deserialized1Value, deserialized2Value);
  }

  @Test
  public void testProgressAfterException() throws Exception {
    final Object initialValue = new Object();
    final Object terminalValue = new Object();
    final Iterator<?> suppliedValues =
        Arrays.asList(new Object(), new RuntimeException(), new Object()).iterator();
    final ExpiringMemoizingSerializableSupplier<?> instance =
        new ExpiringMemoizingSerializableSupplier<>(
            () -> {
              if (!suppliedValues.hasNext()) {
                return terminalValue;
              }
              final Object value = suppliedValues.next();
              if (value instanceof RuntimeException) {
                throw (RuntimeException) value;
              }
              return value;
            },
            Duration.ZERO,
            initialValue,
            Duration.ZERO);

    // The initial value expires immediately and must not be observed.
    final Object instanceValue = instance.get();
    assertNotSame(initialValue, instanceValue);

    // An exception must be thrown for the purpose of this test.
    assertThrows(RuntimeException.class, instance::get);

    // Exceptions must not lock the instance state.
    // The supplied value is referentially distinct from instanceValue for the purpose of this test.
    // Note that parallelly observed supplied values may be referentially equal to instanceValue.
    final Object intermediateValue = instance.get();
    assertNotSame(instanceValue, intermediateValue);

    // The supplied value is referentially equal to terminalValue for the purpose of this test.
    assertSame(terminalValue, instance.get());
  }

  @Test
  public void testInitialValueVisibilityOnDifferentThread() throws Exception {
    final Object initialValue = new Object();
    final Object[] valueHolder = new Object[] {new Object()};
    final ExpiringMemoizingSerializableSupplier<Object> instance =
        new ExpiringMemoizingSerializableSupplier<>(
            Object::new, Duration.ZERO, initialValue, Duration.ofHours(1));

    // Initialization of value and deadlineNanos must be visible on other threads.
    // The initial value must be supplied for delayed expiration.
    final Thread t = new Thread(() -> valueHolder[0] = instance.get());
    t.start();
    t.join();
    final Object observedValue = valueHolder[0];
    assertNotNull(observedValue);
    assertSame(initialValue, observedValue);
  }

  @Test
  public void testIntermediateValueVisibilityOnDifferentThread() throws Exception {
    final Object intermediateValue = new Object();
    final Object[] valueHolder = new Object[] {new Object()};
    final ExpiringMemoizingSerializableSupplier<Object> instance =
        new ExpiringMemoizingSerializableSupplier<>(
            () -> intermediateValue, Duration.ofHours(1), new Object(), Duration.ZERO);

    // Initialization of value and deadlineNanos must be visible on other threads.
    // The intermediate value must be supplied for immediate expiration.
    final Thread t = new Thread(() -> valueHolder[0] = instance.get());
    t.start();
    t.join();
    final Object observedValue = valueHolder[0];
    assertNotNull(observedValue);
    assertSame(intermediateValue, observedValue);
  }
}
