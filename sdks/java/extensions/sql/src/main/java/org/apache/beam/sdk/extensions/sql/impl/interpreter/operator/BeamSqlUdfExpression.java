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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * invoke a UDF function.
 */
public class BeamSqlUdfExpression extends BeamSqlExpression {
  //as Method is not Serializable, need to keep class/method information, and rebuild it.
  private transient Method method;
  private transient Object udfIns;
  private String className;
  private String methodName;
  private List<String> paraClassName = new ArrayList<>();

  public BeamSqlUdfExpression(Method method, List<BeamSqlExpression> subExps,
      SqlTypeName sqlTypeName) {
    super(subExps, sqlTypeName);
    this.method = method;

    this.className = method.getDeclaringClass().getName();
    this.methodName = method.getName();
    for (Class<?> c : method.getParameterTypes()) {
      paraClassName.add(c.getName());
    }
  }

  @Override
  public boolean accept() {
    return true;
  }

  @Override
  public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    if (method == null) {
      reConstructMethod();
    }
    try {
      List<Object> paras = new ArrayList<>();
      for (BeamSqlExpression e : getOperands()) {
        paras.add(e.evaluate(inputRow, window).getValue());
      }

      return BeamSqlPrimitive.of(getOutputType(),
          method.invoke(udfIns, paras.toArray(new Object[]{})));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * re-construct method from class/method.
   */
  private void reConstructMethod() {
    try {
      List<Class<?>> paraClass = new ArrayList<>();
      for (String pc : paraClassName) {
        paraClass.add(Class.forName(pc));
      }
      method = Class.forName(className).getMethod(methodName, paraClass.toArray(new Class<?>[] {}));
      if (!Modifier.isStatic(method.getModifiers())) {
        udfIns = Class.forName(className).newInstance();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
