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

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.AggregateFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** Implement {@link AggregateFunction} to take a {@link CombineFn} as UDAF with one argument. */
@Experimental
@Internal
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public final class SingleArgUdafImpl<InputT, AccumT, OutputT>
    extends AggregateCallCombineFnFactory implements Serializable {
  private CombineFn<InputT, AccumT, OutputT> combineFn;

  public SingleArgUdafImpl(CombineFn<InputT, AccumT, OutputT> combineFn) {
    this.combineFn = combineFn;
  }

  @Override
  public CombineFn<InputT, AccumT, OutputT> getCombineFn() {
    return combineFn;
  }

  private ParameterizedType findCombineFnSuperClass() {
    Class clazz = combineFn.getClass();
    while (!clazz.getSuperclass().equals(CombineFn.class)) {
      clazz = clazz.getSuperclass();
    }

    if (!(clazz.getGenericSuperclass() instanceof ParameterizedType)) {
      throw new IllegalStateException(
          "Subclass of " + CombineFn.class + " must be parameterized to be used as a UDAF");
    } else {
      return (ParameterizedType) clazz.getGenericSuperclass();
    }
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return ImmutableList.of(FunctionParameterBuilder.newBuilder()
        .setOrdinal(0)
        .setType(findCombineFnSuperClass().getActualTypeArguments()[0])
        .build());
  }

  @Override
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return CalciteUtils.sqlTypeWithAutoCast(typeFactory, combineFn.getOutputType().getType());
  }
}
