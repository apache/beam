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
package org.apache.beam.sdk.values;

import java.math.BigDecimal;

/**
 * A utility class containing the Java primitives for
 * {@link TypeDescriptor} equivalents.
 */
public class TypeDescriptors {
  /**
   * The {@link TypeDescriptor} for Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Boolean&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Boolean
   */
  public static TypeDescriptor<Boolean> booleans() {
    return new TypeDescriptor<Boolean>() {};
  }

  /**
   * The {@link TypeDescriptor} for Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Double&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Double
   */
  public static TypeDescriptor<Double> doubles() {
    return new TypeDescriptor<Double>() {};
  }

  /**
   * The {@link TypeDescriptor} for Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Float&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Float
   */
  public static TypeDescriptor<Float> floats() {
    return new TypeDescriptor<Float>() {};
  }

  /**
   * The {@link TypeDescriptor} for Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Integer&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Integer
   */
  public static TypeDescriptor<Integer> integers() {
    return new TypeDescriptor<Integer>() {};
  }

  /**
   * The {@link TypeDescriptor} for Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Long&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Long
   */
  public static TypeDescriptor<Long> longs() {
    return new TypeDescriptor<Long>() {};
  }

  /**
   * The {@link TypeDescriptor} for Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;Short&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for Short
   */
  public static TypeDescriptor<Short> shorts() {
    return new TypeDescriptor<Short>() {};
  }

  /**
   * The {@link TypeDescriptor} for BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;BigDecimal&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for BigDecimal
   */
  public static TypeDescriptor<BigDecimal> bigdecimals() {
    return new TypeDescriptor<BigDecimal>() {};
  }

  /**
   * The {@link TypeDescriptor} for String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;String&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for String
   */
  public static TypeDescriptor<String> strings() {
    return new TypeDescriptor<String>() {};
  }
}
