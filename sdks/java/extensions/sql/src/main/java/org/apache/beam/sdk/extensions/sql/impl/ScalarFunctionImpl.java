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

import static org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.Static.RESOURCE;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMultimap;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.ReflectiveCallNotNullImplementor;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.function.Strict;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.tree.Expression;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.linq4j.tree.Expressions;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Function;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.ImplementableFunction;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.ScalarFunction;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlOperatorBinding;

/**
 * Beam-customized version from {@link
 * org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.impl.ScalarFunctionImpl} , to
 * address BEAM-5921.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class ScalarFunctionImpl extends UdfImplReflectiveFunctionBase
    implements ScalarFunction, ImplementableFunction {

  private final CallImplementor implementor;

  /** Private constructor. */
  private ScalarFunctionImpl(Method method, CallImplementor implementor) {
    super(method);
    this.implementor = implementor;
  }

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Function} for
   * each method in a given class.
   */
  public static ImmutableMultimap<String, Function> createAll(Class<?> clazz) {
    final ImmutableMultimap.Builder<String, Function> builder = ImmutableMultimap.builder();
    for (Method method : clazz.getMethods()) {
      if (method.getDeclaringClass() == Object.class) {
        continue;
      }
      if (!Modifier.isStatic(method.getModifiers()) && !classHasPublicZeroArgsConstructor(clazz)) {
        continue;
      }
      final Function function = create(method);
      builder.put(method.getName(), function);
    }
    return builder.build();
  }

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Function} from
   * given class.
   *
   * <p>If a method of the given name is not found or it does not suit, returns {@code null}.
   *
   * @param clazz class that is used to implement the function
   * @param methodName Method name (typically "eval")
   * @return created {@link ScalarFunction} or null
   */
  public static Function create(Class<?> clazz, String methodName) {
    final Method method = findMethod(clazz, methodName);
    if (method == null) {
      return null;
    }
    return create(method);
  }

  /**
   * Creates {@link org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.schema.Function} from
   * given method. When {@code eval} method does not suit, {@code null} is returned.
   *
   * @param method method that is used to implement the function
   * @return created {@link Function} or null
   */
  public static Function create(Method method) {
    if (!Modifier.isStatic(method.getModifiers())) {
      Class clazz = method.getDeclaringClass();
      if (!classHasPublicZeroArgsConstructor(clazz)) {
        throw RESOURCE.requireDefaultConstructor(clazz.getName()).ex();
      }
    }
    if (method.getExceptionTypes().length != 0) {
      throw new RuntimeException(method.getName() + " must not throw checked exception");
    }

    CallImplementor implementor = createImplementor(method);
    return new ScalarFunctionImpl(method, implementor);
  }

  @Override
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return CalciteUtils.sqlTypeWithAutoCast(typeFactory, method.getGenericReturnType());
  }

  @Override
  public CallImplementor getImplementor() {
    return implementor;
  }

  /**
   * Version of {@link ReflectiveCallNotNullImplementor} that does parameter conversion for Beam
   * UDFs.
   */
  private static class ScalarReflectiveCallNotNullImplementor
      extends ReflectiveCallNotNullImplementor {
    ScalarReflectiveCallNotNullImplementor(Method method) {
      super(method);
    }

    private static List<Expression> translate(List<Type> types, List<Expression> expressions) {
      // https://issues.apache.org/jira/browse/BEAM-8241
      // In user defined functions Calcite allows variants with fewer arguments than Beam defined.
      Preconditions.checkArgument(
          types.size() >= expressions.size(), "types.size() < expressions.size()");

      final List<Expression> translated = new ArrayList<>();
      for (int i = 0; i < expressions.size(); i++) {
        // TODO: [BEAM-8255] Add support for user defined function with var-arg
        // Ex: types: [String[].class], expression: [param1, param2, ...]
        translated.add(translate(types.get(i), expressions.get(i)));
      }

      return translated;
    }

    private static Expression translate(Type type, Expression expression) {
      // NB: base class is called ReflectiveCallNotNullImplementor, but nulls are possible
      //
      // Calcite infers our UDF parameters as nullable, and WILL pass nullable expressions to this
      // method. We could revisit this by explicitly asking users to add @Nullable annotation
      // to UDF parameters, and not treating them as nullable by default, and then we can better
      // determine if expression is possibly nullable by using reflection.

      if (type == byte[].class && expression.type == ByteString.class) {
        return Expressions.condition(
            Expressions.equal(expression, Expressions.constant(null)),
            Expressions.constant(null),
            Expressions.call(expression, "getBytes"));
      }

      return expression;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      final List<Expression> translated =
          translate(Arrays.asList(method.getParameterTypes()), translatedOperands);

      // delegate to the underlying implementation to do the rest of translations
      return super.implement(translator, call, translated);
    }
  }

  private static CallImplementor createImplementor(Method method) {
    final NullPolicy nullPolicy = getNullPolicy(method);
    return RexImpTable.createImplementor(
        new ScalarReflectiveCallNotNullImplementor(method), nullPolicy, false);
  }

  private static NullPolicy getNullPolicy(Method m) {
    if (m.getAnnotation(Strict.class) != null) {
      return NullPolicy.STRICT;
    } else if (m.getAnnotation(SemiStrict.class) != null) {
      return NullPolicy.SEMI_STRICT;
    } else if (m.getDeclaringClass().getAnnotation(Strict.class) != null) {
      return NullPolicy.STRICT;
    } else if (m.getDeclaringClass().getAnnotation(SemiStrict.class) != null) {
      return NullPolicy.SEMI_STRICT;
    } else {
      return NullPolicy.NONE;
    }
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory, SqlOperatorBinding opBinding) {
    // Strict and semi-strict functions can return null even if their Java
    // functions return a primitive type. Because when one of their arguments
    // is null, they won't even be called.
    final RelDataType returnType = getReturnType(typeFactory);
    switch (getNullPolicy(method)) {
      case STRICT:
        for (RelDataType type : opBinding.collectOperandTypes()) {
          if (type.isNullable()) {
            return typeFactory.createTypeWithNullability(returnType, true);
          }
        }
        break;
      case SEMI_STRICT:
        return typeFactory.createTypeWithNullability(returnType, true);
      default:
        break;
    }
    return returnType;
  }

  /**
   * Verifies if given class has public constructor with zero arguments.
   *
   * @param clazz class to verify
   * @return true if given class has public constructor with zero arguments
   */
  static boolean classHasPublicZeroArgsConstructor(Class<?> clazz) {
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getParameterTypes().length == 0
          && Modifier.isPublic(constructor.getModifiers())) {
        return true;
      }
    }
    return false;
  }

  /*
   * Finds a method in a given class by name.
   * @param clazz class to search method in
   * @param name name of the method to find
   * @return the first method with matching name or null when no method found
   */
  static Method findMethod(Class<?> clazz, String name) {
    for (Method method : clazz.getMethods()) {
      if (method.getName().equals(name) && !method.isBridge()) {
        return method;
      }
    }
    return null;
  }
}

// End ScalarFunctionImpl.java
