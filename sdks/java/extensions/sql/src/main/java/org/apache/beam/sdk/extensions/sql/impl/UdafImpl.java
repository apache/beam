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
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.AggregateFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.FunctionParameter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.schema.ImplementableAggFunction;

/** Implement {@link AggregateFunction} to take a {@link CombineFn} as UDAF. */
@Internal
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class UdafImpl<InputT, AccumT, OutputT>
    implements AggregateFunction, ImplementableAggFunction, Serializable {
  private CombineFn<InputT, AccumT, OutputT> combineFn;

  public UdafImpl(CombineFn<InputT, AccumT, OutputT> combineFn) {
    this.combineFn = combineFn;
  }

  public CombineFn<InputT, AccumT, OutputT> getCombineFn() {
    return combineFn;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    List<FunctionParameter> para = new ArrayList<>();
    para.add(
        new FunctionParameter() {
          @Override
          public int getOrdinal() {
            return 0; // up to one parameter is supported in UDAF.
          }

          @Override
          public String getName() {
            // not used as Beam SQL uses its own execution engine
            return null;
          }

          @Override
          public RelDataType getType(RelDataTypeFactory typeFactory) {
            Type inputType = getInputType();
            if (inputType instanceof TypeVariable) {
              throw new IllegalArgumentException(
                  "Unable to infer SQL type from type variable "
                      + inputType
                      + ". This usually means you are trying to use a generic type whose type information "
                      + "is not known at runtime. You can wrap your CombineFn into typed subclass"
                      + " by 'new TypedCombineFnDelegate<...>(combineFn) {}'");
            }
            return CalciteUtils.sqlTypeWithAutoCast(typeFactory, inputType);
          }

          @Override
          public boolean isOptional() {
            // not used as Beam SQL uses its own execution engine
            return false;
          }
        });
    return para;
  }

  @Override
  public AggImplementor getImplementor(boolean windowContext) {
    // not used as Beam SQL uses its own execution engine
    return null;
  }

  @Override
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return CalciteUtils.sqlTypeWithAutoCast(typeFactory, getOutputType());
  }

  protected Type getInputType() {
    @Nullable Type inputType = combineFn.getInputType().getType();
    if (inputType != null && !(inputType instanceof TypeVariable)) {
      return inputType;
    }
    ParameterizedType parameterizedType = findCombineFnSuperClass();
    return parameterizedType.getActualTypeArguments()[0];
  }

  protected Type getOutputType() {
    return combineFn.getOutputType().getType();
  }

  private ParameterizedType findCombineFnSuperClass() {

    Class clazz = combineFn.getClass();

    while (!clazz.getSuperclass().equals(CombineFn.class)) {
      clazz = clazz.getSuperclass();
    }

    if (!(clazz.getGenericSuperclass() instanceof ParameterizedType)) {
      throw new IllegalStateException(
          "Subclass of " + CombineFn.class + " must be parameterized to be used as a UDAF");
    }
    return (ParameterizedType) clazz.getGenericSuperclass();
  }
}
