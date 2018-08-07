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

import java.lang.reflect.TypeVariable;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Assert;

/** Buncg of methods to assert type descriptors in operators. */
public class TypePropagationAssert {

  public static void assertOperatorTypeAwareness(
      Operator<?, ?> operator,
      TypeDescriptor<?> outputType,
      TypeDescriptor<?> keyType,
      TypeDescriptor<?> valueType) {

    if (keyType != null || operator instanceof TypeAware.Key) {
      Assert.assertSame(keyType, ((TypeAware.Key) operator).getKeyType());

      TypeDescriptor<KV<?, ?>> kvOutputType = (TypeDescriptor<KV<?, ?>>) operator.getOutputType();

      TypeVariable<Class<KV>>[] kvParameters = KV.class.getTypeParameters();

      TypeDescriptor<?> firstType = kvOutputType.resolveType(kvParameters[0]);
      TypeDescriptor<?> secondType = kvOutputType.resolveType(kvParameters[1]);

      Assert.assertEquals(keyType, firstType);
      Assert.assertEquals(outputType, secondType);

    } else {
      // assert output of non keyed operator
      Assert.assertSame(outputType, operator.getOutputType());
    }

    if (valueType != null || operator instanceof TypeAware.Value) {
      Assert.assertSame(valueType, ((TypeAware.Value) operator).getValueType());
    }
  }

  public static void assertOperatorTypeAwareness(
      Operator<?, ?> operator, TypeDescriptor<?> outputType) {
    assertOperatorTypeAwareness(operator, outputType, null, null);
  }

  public static void assertOperatorTypeAwareness(
      Operator<?, ?> operator, TypeDescriptor<?> outputType, TypeDescriptor<?> keyType) {
    assertOperatorTypeAwareness(operator, outputType, keyType, null);
  }
}
