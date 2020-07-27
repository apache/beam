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
package org.apache.beam.sdk.extensions.euphoria.core.client.type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.TypeVariable;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.ShuffleOperator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Bunch of methods to assert type descriptors in operators. */
public class TypePropagationAssert {

  public static <KeyT, ValueT, OutputT> void assertOperatorTypeAwareness(
      Operator<OutputT> operator,
      @Nullable TypeDescriptor<KeyT> keyType,
      @Nullable TypeDescriptor<ValueT> valueType,
      TypeDescriptor<OutputT> outputType) {
    if (keyType != null || operator instanceof TypeAware.Key) {
      @SuppressWarnings("unchecked")
      final TypeAware.Key<KeyT> keyAware = (TypeAware.Key) operator;
      assertTrue(keyAware.getKeyType().isPresent());
      assertEquals(keyType, keyAware.getKeyType().get());
      assertTrue(operator.getOutputType().isPresent());
      @SuppressWarnings("unchecked")
      final TypeDescriptor<KV<KeyT, OutputT>> kvOutputType =
          (TypeDescriptor) operator.getOutputType().get();
      final TypeVariable<Class<KV>>[] kvParameters = KV.class.getTypeParameters();
      final TypeDescriptor<?> firstType = kvOutputType.resolveType(kvParameters[0]);
      final TypeDescriptor<?> secondType = kvOutputType.resolveType(kvParameters[1]);
      assertEquals(keyType, firstType);
      assertEquals(outputType, secondType);
    } else {
      // assert output of non keyed operator
      assertEquals(outputType, operator.getOutputType().get());
    }
    if (valueType != null || operator instanceof TypeAware.Value) {
      @SuppressWarnings("unchecked")
      final TypeAware.Value<KeyT> valueAware = (TypeAware.Value) operator;
      assertTrue(valueAware.getValueType().isPresent());
      assertEquals(valueType, valueAware.getValueType().get());
    }
  }

  public static <OutputT> void assertOperatorTypeAwareness(
      Operator<OutputT> operator, TypeDescriptor<OutputT> outputType) {
    assertOperatorTypeAwareness(operator, null, null, outputType);
  }

  public static <KeyT, OutputT> void assertOperatorTypeAwareness(
      ShuffleOperator<?, KeyT, OutputT> operator,
      TypeDescriptor<KeyT> keyType,
      TypeDescriptor<OutputT> outputType) {
    assertOperatorTypeAwareness(operator, keyType, null, outputType);
  }
}
