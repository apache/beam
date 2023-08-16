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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.sql.udf.AggregateFn;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.AggregateFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

/** Tests for {@link LazyAggregateCombineFn}. */
@RunWith(JUnit4.class)
public class LazyAggregateCombineFnTest {
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void getAccumulatorCoderInfersCoderForWildcardTypeParameter()
      throws CannotProvideCoderException {
    LazyAggregateCombineFn<Long, ?, ?> combiner = new LazyAggregateCombineFn<>(new Sum());
    Coder<?> coder = combiner.getAccumulatorCoder(CoderRegistry.createDefault(), VarLongCoder.of());
    assertThat(coder, instanceOf(VarLongCoder.class));
  }

  @Test
  public void mergeAccumulators() {
    LazyAggregateCombineFn<Long, Long, Long> combiner = new LazyAggregateCombineFn<>(new Sum());
    long merged = combiner.mergeAccumulators(ImmutableList.of(1L, 1L));
    assertEquals(2L, merged);
  }

  @RunWith(Parameterized.class)
  public static class UdafImplTest {
    @Parameterized.Parameters(name = "aggregateFn: {0}")
    public static Object[] data() {
      return new Object[] {new Sum(), new SumChild()};
    }

    @Parameterized.Parameter public AggregateFn aggregateFn;

    @Test
    public void subclassGetUdafImpl() {
      LazyAggregateCombineFn<?, ?, ?> combiner = new LazyAggregateCombineFn<>(aggregateFn);
      AggregateFunction aggregateFunction = combiner.getUdafImpl();
      RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      RelDataType expectedType = typeFactory.createJavaType(Long.class);

      List<FunctionParameter> params = aggregateFunction.getParameters();
      assertThat(params, hasSize(1));
      RelDataType paramType = params.get(0).getType(typeFactory);
      assertEquals(expectedType, paramType);

      RelDataType returnType = aggregateFunction.getReturnType(typeFactory);
      assertEquals(expectedType, returnType);
    }
  }

  @Test
  public void nonparameterizedGetUdafImpl_throwsIllegalStateException() {
    LazyAggregateCombineFn<?, ?, ?> combiner =
        new LazyAggregateCombineFn<>(new NonParameterizedAggregateFn());
    AggregateFunction aggregateFunction = combiner.getUdafImpl();
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    exceptions.expect(IllegalStateException.class);

    List<FunctionParameter> params = aggregateFunction.getParameters();
    params.get(0).getType(typeFactory);
  }

  public static class Sum implements AggregateFn<Long, Long, Long> {

    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long addInput(Long mutableAccumulator, Long input) {
      return mutableAccumulator + input;
    }

    @Override
    public Long mergeAccumulators(Long mutableAccumulator, Iterable<Long> immutableAccumulators) {
      for (Long x : immutableAccumulators) {
        mutableAccumulator += x;
      }
      return mutableAccumulator;
    }

    @Override
    public Long extractOutput(Long mutableAccumulator) {
      return mutableAccumulator;
    }
  }

  public static class SumChild extends Sum {}

  public static class NonParameterizedAggregateFn implements AggregateFn {

    @Override
    public @Nullable Object createAccumulator() {
      return null;
    }

    @Override
    public @Nullable Object addInput(Object mutableAccumulator, Object input) {
      return null;
    }

    @Override
    public @Nullable Object mergeAccumulators(
        Object mutableAccumulator, Iterable immutableAccumulators) {
      return null;
    }

    @Override
    public @Nullable Object extractOutput(Object mutableAccumulator) {
      return null;
    }
  }
}
