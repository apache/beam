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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableAggFunction;

/**
 * Implement {@link AggregateFunction} to take a {@link CombineFn} as UDAF.
 */
public final class UdafImpl<InputT, AccumT, OutputT>
    implements AggregateFunction, ImplementableAggFunction, Serializable{
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
    para.add(new FunctionParameter() {
          public int getOrdinal() {
            return 0; //up to one parameter is supported in UDAF.
          }

          public String getName() {
            // not used as Beam SQL uses its own execution engine
            return null;
          }

          public RelDataType getType(RelDataTypeFactory typeFactory) {
            ParameterizedType parameterizedType = findCombineFnSuperClass();
            return typeFactory.createJavaType(
              (Class) parameterizedType.getActualTypeArguments()[0]);
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
    return typeFactory.createJavaType((Class) combineFn.getOutputType().getType());
  }
}

