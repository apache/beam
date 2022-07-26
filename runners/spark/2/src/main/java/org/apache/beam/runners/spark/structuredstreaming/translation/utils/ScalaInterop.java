package org.apache.beam.runners.spark.structuredstreaming.translation.utils;

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
  
  public static <T> Seq<T> listOf(T t) {
    return emptyList().$colon$colon(t);
  }

  public static <T> List<T> emptyList() {
    return (List<T>) Nil$.MODULE$;
  }

}
