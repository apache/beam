/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.euphoria.core.client.type;

import java.lang.invoke.MethodType;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Either;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

/**
 * A collections of {@link TypeDescriptor} construction methods.
 */
public class TypeUtils {


  /**
   * Creates composite {@link TypeDescriptor} of {@code <Pair<K,V>}. Provided that both given
   * parameters are non null.
   *
   * @param key key type descriptor
   * @param value value type descriptor
   * @param <K> key tye parameter
   * @param <V> value type parameter
   * @return {@link TypeDescriptor} of {@code <Pair<K,V>} when {@code key} and {@code value} are not
   * null, null otherwise
   */
  @Nullable
  public static <K, V> TypeDescriptor<Pair<K, V>> pairs(
      TypeDescriptor<K> key, TypeDescriptor<V> value) {

    if (Objects.isNull(key) || Objects.isNull(value)) {
      return null;
    }

    return new TypeDescriptor<Pair<K, V>>() {
    }.where(new TypeParameter<K>() {
    }, key)
        .where(new TypeParameter<V>() {
        }, value);
  }

  /**
   * Creates composite {@link TypeDescriptor} of {@code <Pair<K,V>}. Provided that both given
   * parameters are non null.
   *
   * @param key key type descriptor
   * @param value value type descriptor
   * @param <K> key type parameter
   * @param <V> value type parameter
   * @return {@link TypeDescriptor} of {@code <Pair<K,V>} when {@code key} and {@code value} are not
   * null, null otherwise
   */
  @Nullable
  public static <K, V> TypeDescriptor<Pair<K, V>> pairs(
      Class<K> key, Class<V> value) {

    if (Objects.isNull(key) || Objects.isNull(value)) {
      return null;
    }

    return pairs(TypeDescriptor.of(key), TypeDescriptor.of(value));
  }

  /**
   * Creates composite {@link TypeDescriptor} of {@code <Triple<K,V, ScoreT>}. Provided that all
   * given parameters are non null.
   *
   * @param key key type descriptor
   * @param value value type descriptor
   * @param score score type descriptor
   * @param <K> key type parameter
   * @param <V> value type parameter
   * @param <ScoreT> score type parameter
   * @return {@link TypeDescriptor} of {@code <Triple<K,V, ScoreT>} or {@code null} when any given
   * parameter is {@code null}
   */
  public static <K, V, ScoreT> TypeDescriptor<Triple<K, V, ScoreT>> triplets(
      TypeDescriptor<K> key, TypeDescriptor<V> value, TypeDescriptor<ScoreT> score
  ) {

    if (Objects.isNull(key) || Objects.isNull(value) || Objects.isNull(score)) {
      return null;
    }

    return new TypeDescriptor<Triple<K, V, ScoreT>>() {
    }.where(new TypeParameter<K>() {
    }, key)
        .where(new TypeParameter<V>() {
        }, value)
        .where(new TypeParameter<ScoreT>() {
        }, score);
  }

  /**
   * Creates composite {@link TypeDescriptor} of {@code Either<K,V>}. Provided that all given
   * parameters are non null.
   *
   * @param key key type descriptor
   * @param value value type descriptor
   * @param <K> key type parameter
   * @param <V> value type parameter
   * @return {@link TypeDescriptor} of {@code <Either<K,V>} or {@code null} when any given parameter
   * is {@code null}
   */
  public static <K, V> TypeDescriptor<Either<K, V>> eithers(
      TypeDescriptor<K> key, TypeDescriptor<V> value) {

    if (Objects.isNull(key) || Objects.isNull(value)) {
      return null;
    }

    return new TypeDescriptor<Either<K, V>>() {
    }.where(new TypeParameter<K>() {
    }, key)
        .where(new TypeParameter<V>() {
        }, value);
  }

  /**
   * Returns {@link TypeDescriptor} od elements in given {@code dataset} if available, {@code null}
   * otherwise.
   * <p>
   * {@link TypeDescriptor} is acquired as outpyut type of the {@link Operator} which is a producer
   * of given {@link Dataset}.
   * </p>
   */
  @Nullable
  public static <T> TypeDescriptor<T> getDatasetElementType(Dataset<T> dataset) {
    if (dataset == null) {
      return null;
    }

    Operator<?, T> producer = dataset.getProducer();
    if (producer == null) {
      return null;
    }

    return producer.getOutputType();
  }

  /**
   * Get return type of lambda function (e.g UnaryFunction).
   *
   * @return return type
   */ //TODO update javadoc
  public static Type getLambdaReturnType(final Object lambda) {

    Type returnType = null;
    try {
      //try to obtain SerializedLambda through invoking writeReplace()
      final Method method = lambda.getClass().getDeclaredMethod("writeReplace");
      method.setAccessible(true);
      final SerializedLambda serializedLambda = SerializedLambda.class.cast(method.invoke(lambda));
      final String signature = serializedLambda.getImplMethodSignature();
      final MethodType methodType =
          MethodType.fromMethodDescriptorString(signature, lambda.getClass().getClassLoader());
      returnType = methodType.returnType();

    } catch (IllegalAccessException | NoSuchMethodException | IllegalArgumentException
        | InvocationTargetException | ClassCastException e){
      // not a lambda try pure reflection, algo is trivial since it is functional interfaces: take
      // the only not Object methods ;)
      for (final Method m : lambda.getClass().getMethods()) {
        if (Object.class == m.getDeclaringClass()) {
          continue;
        }
        returnType = m.getReturnType();
        break;
      }
    }
    return returnType;
  }
}
