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
 * A utility class containing the {@link TypeDescriptor} classes for
 * {@link KV} using common Java primitives.
 */
public class KVTypeDescriptors {
  /**
   * The {@link TypeDescriptor} for {@link KV} of type Boolean and Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Boolean,Boolean&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Boolean and Boolean
   */
  public static TypeDescriptor<KV<Boolean, Boolean>>
    booleansBooleans() {
    return new TypeDescriptor<KV<Boolean, Boolean>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Boolean and Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Boolean,Double&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Boolean and Double
   */
  public static TypeDescriptor<KV<Boolean, Double>>
    booleansDoubles() {
    return new TypeDescriptor<KV<Boolean, Double>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Boolean and Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Boolean,Float&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Boolean and Float
   */
  public static TypeDescriptor<KV<Boolean, Float>>
    booleansFloats() {
    return new TypeDescriptor<KV<Boolean, Float>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Boolean and Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Boolean,Integer&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Boolean and Integer
   */
  public static TypeDescriptor<KV<Boolean, Integer>>
    booleansIntegers() {
    return new TypeDescriptor<KV<Boolean, Integer>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Boolean and Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Boolean,Long&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Boolean and Long
   */
  public static TypeDescriptor<KV<Boolean, Long>>
    booleansLongs() {
    return new TypeDescriptor<KV<Boolean, Long>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Boolean and Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Boolean,Short&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Boolean and Short
   */
  public static TypeDescriptor<KV<Boolean, Short>>
    booleansShorts() {
    return new TypeDescriptor<KV<Boolean, Short>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Boolean and BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Boolean,BigDecimal&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Boolean and BigDecimal
   */
  public static TypeDescriptor<KV<Boolean, BigDecimal>>
    booleansBigDecimals() {
    return new TypeDescriptor<KV<Boolean, BigDecimal>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Boolean and String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Boolean,String&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Boolean and String
   */
  public static TypeDescriptor<KV<Boolean, String>>
    booleansStrings() {
    return new TypeDescriptor<KV<Boolean, String>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Double and Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Double,Boolean&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Double and Boolean
   */
  public static TypeDescriptor<KV<Double, Boolean>>
    doublesBooleans() {
    return new TypeDescriptor<KV<Double, Boolean>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Double and Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Double,Double&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Double and Double
   */
  public static TypeDescriptor<KV<Double, Double>>
    doublesDoubles() {
    return new TypeDescriptor<KV<Double, Double>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Double and Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Double,Float&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Double and Float
   */
  public static TypeDescriptor<KV<Double, Float>>
    doublesFloats() {
    return new TypeDescriptor<KV<Double, Float>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Double and Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Double,Integer&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Double and Integer
   */
  public static TypeDescriptor<KV<Double, Integer>>
    doublesIntegers() {
    return new TypeDescriptor<KV<Double, Integer>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Double and Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Double,Long&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Double and Long
   */
  public static TypeDescriptor<KV<Double, Long>>
    doublesLongs() {
    return new TypeDescriptor<KV<Double, Long>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Double and Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Double,Short&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Double and Short
   */
  public static TypeDescriptor<KV<Double, Short>>
    doublesShorts() {
    return new TypeDescriptor<KV<Double, Short>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Double and BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Double,BigDecimal&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Double and BigDecimal
   */
  public static TypeDescriptor<KV<Double, BigDecimal>>
    doublesBigDecimals() {
    return new TypeDescriptor<KV<Double, BigDecimal>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Double and String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Double,String&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Double and String
   */
  public static TypeDescriptor<KV<Double, String>>
    doublesStrings() {
    return new TypeDescriptor<KV<Double, String>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Float and Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Float,Boolean&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Float and Boolean
   */
  public static TypeDescriptor<KV<Float, Boolean>>
    floatsBooleans() {
    return new TypeDescriptor<KV<Float, Boolean>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Float and Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Float,Double&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Float and Double
   */
  public static TypeDescriptor<KV<Float, Double>>
    floatsDoubles() {
    return new TypeDescriptor<KV<Float, Double>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Float and Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Float,Float&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Float and Float
   */
  public static TypeDescriptor<KV<Float, Float>>
    floatsFloats() {
    return new TypeDescriptor<KV<Float, Float>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Float and Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Float,Integer&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Float and Integer
   */
  public static TypeDescriptor<KV<Float, Integer>>
    floatsIntegers() {
    return new TypeDescriptor<KV<Float, Integer>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Float and Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Float,Long&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Float and Long
   */
  public static TypeDescriptor<KV<Float, Long>>
    floatsLongs() {
    return new TypeDescriptor<KV<Float, Long>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Float and Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Float,Short&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Float and Short
   */
  public static TypeDescriptor<KV<Float, Short>>
    floatsShorts() {
    return new TypeDescriptor<KV<Float, Short>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Float and BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Float,BigDecimal&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Float and BigDecimal
   */
  public static TypeDescriptor<KV<Float, BigDecimal>>
    floatsBigDecimals() {
    return new TypeDescriptor<KV<Float, BigDecimal>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Float and String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Float,String&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Float and String
   */
  public static TypeDescriptor<KV<Float, String>>
    floatsStrings() {
    return new TypeDescriptor<KV<Float, String>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Integer and Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Integer,Boolean&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Integer and Boolean
   */
  public static TypeDescriptor<KV<Integer, Boolean>>
    integersBooleans() {
    return new TypeDescriptor<KV<Integer, Boolean>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Integer and Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Integer,Double&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Integer and Double
   */
  public static TypeDescriptor<KV<Integer, Double>>
    integersDoubles() {
    return new TypeDescriptor<KV<Integer, Double>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Integer and Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Integer,Float&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Integer and Float
   */
  public static TypeDescriptor<KV<Integer, Float>>
    integersFloats() {
    return new TypeDescriptor<KV<Integer, Float>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Integer and Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Integer,Integer&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Integer and Integer
   */
  public static TypeDescriptor<KV<Integer, Integer>>
    integersIntegers() {
    return new TypeDescriptor<KV<Integer, Integer>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Integer and Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Integer,Long&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Integer and Long
   */
  public static TypeDescriptor<KV<Integer, Long>>
    integersLongs() {
    return new TypeDescriptor<KV<Integer, Long>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Integer and Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Integer,Short&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Integer and Short
   */
  public static TypeDescriptor<KV<Integer, Short>>
    integersShorts() {
    return new TypeDescriptor<KV<Integer, Short>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Integer and BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Integer,BigDecimal&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Integer and BigDecimal
   */
  public static TypeDescriptor<KV<Integer, BigDecimal>>
    integersBigDecimals() {
    return new TypeDescriptor<KV<Integer, BigDecimal>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Integer and String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Integer,String&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Integer and String
   */
  public static TypeDescriptor<KV<Integer, String>>
    integersStrings() {
    return new TypeDescriptor<KV<Integer, String>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Long and Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Long,Boolean&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Long and Boolean
   */
  public static TypeDescriptor<KV<Long, Boolean>>
    longsBooleans() {
    return new TypeDescriptor<KV<Long, Boolean>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Long and Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Long,Double&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Long and Double
   */
  public static TypeDescriptor<KV<Long, Double>>
    longsDoubles() {
    return new TypeDescriptor<KV<Long, Double>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Long and Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Long,Float&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Long and Float
   */
  public static TypeDescriptor<KV<Long, Float>>
    longsFloats() {
    return new TypeDescriptor<KV<Long, Float>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Long and Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Long,Integer&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Long and Integer
   */
  public static TypeDescriptor<KV<Long, Integer>>
    longsIntegers() {
    return new TypeDescriptor<KV<Long, Integer>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Long and Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Long,Long&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Long and Long
   */
  public static TypeDescriptor<KV<Long, Long>>
    longsLongs() {
    return new TypeDescriptor<KV<Long, Long>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Long and Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Long,Short&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Long and Short
   */
  public static TypeDescriptor<KV<Long, Short>>
    longsShorts() {
    return new TypeDescriptor<KV<Long, Short>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Long and BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Long,BigDecimal&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Long and BigDecimal
   */
  public static TypeDescriptor<KV<Long, BigDecimal>>
    longsBigDecimals() {
    return new TypeDescriptor<KV<Long, BigDecimal>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Long and String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Long,String&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Long and String
   */
  public static TypeDescriptor<KV<Long, String>>
    longsStrings() {
    return new TypeDescriptor<KV<Long, String>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Short and Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Short,Boolean&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Short and Boolean
   */
  public static TypeDescriptor<KV<Short, Boolean>>
    shortsBooleans() {
    return new TypeDescriptor<KV<Short, Boolean>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Short and Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Short,Double&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Short and Double
   */
  public static TypeDescriptor<KV<Short, Double>>
    shortsDoubles() {
    return new TypeDescriptor<KV<Short, Double>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Short and Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Short,Float&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Short and Float
   */
  public static TypeDescriptor<KV<Short, Float>>
    shortsFloats() {
    return new TypeDescriptor<KV<Short, Float>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Short and Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Short,Integer&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Short and Integer
   */
  public static TypeDescriptor<KV<Short, Integer>>
    shortsIntegers() {
    return new TypeDescriptor<KV<Short, Integer>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Short and Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Short,Long&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Short and Long
   */
  public static TypeDescriptor<KV<Short, Long>>
    shortsLongs() {
    return new TypeDescriptor<KV<Short, Long>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Short and Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Short,Short&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Short and Short
   */
  public static TypeDescriptor<KV<Short, Short>>
    shortsShorts() {
    return new TypeDescriptor<KV<Short, Short>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Short and BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Short,BigDecimal&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Short and BigDecimal
   */
  public static TypeDescriptor<KV<Short, BigDecimal>>
    shortsBigDecimals() {
    return new TypeDescriptor<KV<Short, BigDecimal>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type Short and String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;Short,String&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type Short and String
   */
  public static TypeDescriptor<KV<Short, String>>
    shortsStrings() {
    return new TypeDescriptor<KV<Short, String>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type BigDecimal and Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;BigDecimal,Boolean&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type BigDecimal and Boolean
   */
  public static TypeDescriptor<KV<BigDecimal, Boolean>>
    bigdecimalsBooleans() {
    return new TypeDescriptor<KV<BigDecimal, Boolean>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type BigDecimal and Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;BigDecimal,Double&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type BigDecimal and Double
   */
  public static TypeDescriptor<KV<BigDecimal, Double>>
    bigdecimalsDoubles() {
    return new TypeDescriptor<KV<BigDecimal, Double>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type BigDecimal and Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;BigDecimal,Float&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type BigDecimal and Float
   */
  public static TypeDescriptor<KV<BigDecimal, Float>>
    bigdecimalsFloats() {
    return new TypeDescriptor<KV<BigDecimal, Float>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type BigDecimal and Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;BigDecimal,Integer&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type BigDecimal and Integer
   */
  public static TypeDescriptor<KV<BigDecimal, Integer>>
    bigdecimalsIntegers() {
    return new TypeDescriptor<KV<BigDecimal, Integer>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type BigDecimal and Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;BigDecimal,Long&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type BigDecimal and Long
   */
  public static TypeDescriptor<KV<BigDecimal, Long>>
    bigdecimalsLongs() {
    return new TypeDescriptor<KV<BigDecimal, Long>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type BigDecimal and Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;BigDecimal,Short&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type BigDecimal and Short
   */
  public static TypeDescriptor<KV<BigDecimal, Short>>
    bigdecimalsShorts() {
    return new TypeDescriptor<KV<BigDecimal, Short>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type BigDecimal and BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;BigDecimal,BigDecimal&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type BigDecimal and BigDecimal
   */
  public static TypeDescriptor<KV<BigDecimal, BigDecimal>>
    bigdecimalsBigDecimals() {
    return new TypeDescriptor<KV<BigDecimal, BigDecimal>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type BigDecimal and String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;BigDecimal,String&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type BigDecimal and String
   */
  public static TypeDescriptor<KV<BigDecimal, String>>
    bigdecimalsStrings() {
    return new TypeDescriptor<KV<BigDecimal, String>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type String and Boolean.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;String,Boolean&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type String and Boolean
   */
  public static TypeDescriptor<KV<String, Boolean>>
    stringsBooleans() {
    return new TypeDescriptor<KV<String, Boolean>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type String and Double.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;String,Double&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type String and Double
   */
  public static TypeDescriptor<KV<String, Double>>
    stringsDoubles() {
    return new TypeDescriptor<KV<String, Double>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type String and Float.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;String,Float&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type String and Float
   */
  public static TypeDescriptor<KV<String, Float>>
    stringsFloats() {
    return new TypeDescriptor<KV<String, Float>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type String and Integer.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;String,Integer&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type String and Integer
   */
  public static TypeDescriptor<KV<String, Integer>>
    stringsIntegers() {
    return new TypeDescriptor<KV<String, Integer>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type String and Long.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;String,Long&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type String and Long
   */
  public static TypeDescriptor<KV<String, Long>>
    stringsLongs() {
    return new TypeDescriptor<KV<String, Long>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type String and Short.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;String,Short&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type String and Short
   */
  public static TypeDescriptor<KV<String, Short>>
    stringsShorts() {
    return new TypeDescriptor<KV<String, Short>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type String and BigDecimal.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;String,BigDecimal&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type String and BigDecimal
   */
  public static TypeDescriptor<KV<String, BigDecimal>>
    stringsBigDecimals() {
    return new TypeDescriptor<KV<String, BigDecimal>>() {};
  }

  /**
   * The {@link TypeDescriptor} for {@link KV} of type String and String.
   * This is the equivalent of:
   * <pre>
   * new TypeDescriptor&lt;KV&lt;String,String&gt;&gt;() {};
   * </pre>
   * @return A {@link TypeDescriptor} for {@link KV} of type String and String
   */
  public static TypeDescriptor<KV<String, String>>
    stringsStrings() {
    return new TypeDescriptor<KV<String, String>>() {};
  }
}
