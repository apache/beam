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
package org.apache.beam.runners.spark.structuredstreaming.translation.utils;

import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.NonNull;
import scala.Function1;
import scala.Function2;
import scala.PartialFunction;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.Nil$;
import scala.collection.mutable.WrappedArray;

/** Utilities for easier interoperability with the Spark Scala API. */
public class ScalaInterop {
  private ScalaInterop() {}

  public static <T> Seq<T> seqOf(T... t) {
    return new WrappedArray.ofRef<>(t);
  }

  public static <T> List<T> concat(List<T> a, List<T> b) {
    return b.$colon$colon$colon(a);
  }

  public static <T> Seq<T> listOf(T t) {
    return emptyList().$colon$colon(t);
  }

  public static <T> List<T> emptyList() {
    return (List<T>) Nil$.MODULE$;
  }

  /** Scala {@link Iterator} of Java {@link Iterable}. */
  public static <T extends @NonNull Object> Iterator<T> scalaIterator(Iterable<T> iterable) {
    return scalaIterator(iterable.iterator());
  }

  /** Scala {@link Iterator} of Java {@link java.util.Iterator}. */
  public static <T extends @NonNull Object> Iterator<T> scalaIterator(java.util.Iterator<T> it) {
    return JavaConverters.asScalaIterator(it);
  }

  /** Java {@link java.util.Iterator} of Scala {@link Iterator}. */
  public static <T extends @NonNull Object> java.util.Iterator<T> javaIterator(Iterator<T> it) {
    return JavaConverters.asJavaIterator(it);
  }

  public static <T1, T2> Tuple2<T1, T2> tuple(T1 t1, T2 t2) {
    return new Tuple2<>(t1, t2);
  }

  public static <T extends @NonNull Object, V> PartialFunction<T, T> replace(
      Class<V> clazz, T replace) {
    return new PartialFunction<T, T>() {

      @Override
      public boolean isDefinedAt(T x) {
        return clazz.isAssignableFrom(x.getClass());
      }

      @Override
      public T apply(T x) {
        return replace;
      }
    };
  }

  public static <T extends @NonNull Object, V> PartialFunction<T, V> match(Class<V> clazz) {
    return new PartialFunction<T, V>() {

      @Override
      public boolean isDefinedAt(T x) {
        return clazz.isAssignableFrom(x.getClass());
      }

      @Override
      public V apply(T x) {
        return (V) x;
      }
    };
  }

  public static <T, V> Fun1<T, V> fun1(Fun1<T, V> fun) {
    return fun;
  }

  public static <T1, T2, V> Fun2<T1, T2, V> fun2(Fun2<T1, T2, V> fun) {
    return fun;
  }

  public interface Fun1<T, V> extends Function1<T, V>, Serializable {}

  public interface Fun2<T1, T2, V> extends Function2<T1, T2, V>, Serializable {}
}
