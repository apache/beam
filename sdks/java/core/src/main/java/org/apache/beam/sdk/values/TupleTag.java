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

import java.io.Serializable;
import java.util.Random;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultiset;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multiset;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link TupleTag} is a typed tag to use as the key of a heterogeneously typed tuple, like {@link
 * PCollectionTuple}. Its generic type parameter allows tracking the static type of things stored in
 * tuples.
 *
 * <p>To aid in assigning default {@link org.apache.beam.sdk.coders.Coder Coders} for results of a
 * {@link ParDo}, an output {@link TupleTag} should be instantiated with an extra {@code {}} so it
 * is an instance of an anonymous subclass without generic type parameters. Input {@link TupleTag
 * TupleTags} require no such extra instantiation (although it doesn't hurt). For example:
 *
 * <pre>{@code
 * TupleTag<SomeType> inputTag = new TupleTag<>();
 * TupleTag<SomeOtherType> outputTag = new TupleTag<SomeOtherType>(){};
 * }</pre>
 *
 * @param <V> the type of the elements or values of the tagged thing, e.g., a {@code
 *     PCollection<V>}.
 */
public class TupleTag<V> implements Serializable {
  /**
   * Constructs a new {@code TupleTag}, with a fresh unique id.
   *
   * <p>This is the normal way {@code TupleTag}s are constructed.
   */
  public TupleTag() {
    this(genId(), true);
  }

  /**
   * Constructs a new {@code TupleTag} with the given id.
   *
   * <p>It is up to the user to ensure that two {@code TupleTag}s with the same id actually mean the
   * same tag and carry the same generic type parameter. Violating this invariant can lead to
   * hard-to-diagnose runtime type errors. Consequently, this operation should be used very
   * sparingly, such as when the producer and consumer of {@code TupleTag}s are written in separate
   * modules and can only coordinate via ids rather than shared {@code TupleTag} instances. Most of
   * the time, {@link #TupleTag()} should be preferred.
   */
  public TupleTag(String id) {
    this(id, false);
  }

  /**
   * Returns the id of this {@code TupleTag}.
   *
   * <p>Two {@code TupleTag}s with the same id are considered equal.
   *
   * <p>{@code TupleTag}s are not ordered, i.e., the class does not implement Comparable interface.
   * TupleTags implement equals and hashCode, making them suitable for use as keys in HashMap and
   * HashSet.
   */
  public String getId() {
    return id;
  }

  /**
   * If this {@code TupleTag} is tagging output {@code outputIndex} of a {@code PTransform}, returns
   * the name that should be used by default for the output.
   */
  public String getOutName(int outIndex) {
    if (generated) {
      return "out" + outIndex;
    } else {
      return id;
    }
  }

  /**
   * Returns a {@code TypeDescriptor} capturing what is known statically about the type of this
   * {@code TupleTag} instance's most-derived class.
   *
   * <p>This is useful for a {@code TupleTag} constructed as an instance of an anonymous subclass
   * with a trailing {@code {}}, e.g., {@code new TupleTag<SomeType>(){}}.
   */
  public TypeDescriptor<V> getTypeDescriptor() {
    return new TypeDescriptor<V>(getClass()) {};
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal details below here.

  static final Random RANDOM = new Random(0);
  private static final Multiset<String> staticInits = HashMultiset.create();

  final String id;
  final boolean generated;

  /** Generates and returns a fresh unique id for a TupleTag's id. */
  static synchronized String genId() {
    // It is a common pattern to store tags that are shared between the main
    // program and workers in static variables, but such references are not
    // serialized as part of the *Fns state.  Fortunately, most such tags
    // are constructed in static class initializers, e.g.
    //
    //     static final TupleTag<T> MY_TAG = new TupleTag<>();
    //
    // and class initialization order is well defined by the JVM spec, so in
    // this case we can assign deterministic ids.
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    for (StackTraceElement frame : stackTrace) {
      if ("<clinit>".equals(frame.getMethodName())) {
        int counter = staticInits.add(frame.getClassName(), 1);
        return frame.getClassName() + "#" + counter;
      }
    }
    // Otherwise, assume it'll be serialized and choose a random value to reduce
    // the chance of collision.
    String nonce = Long.toHexString(RANDOM.nextLong());
    // [Thread.getStackTrace, TupleTag.getId, TupleTag.<init>, caller, ...]
    String caller =
        stackTrace.length >= 4
            ? stackTrace[3].getClassName()
                + "."
                + stackTrace[3].getMethodName()
                + ":"
                + stackTrace[3].getLineNumber()
            : "unknown";
    return caller + "#" + nonce;
  }

  private TupleTag(String id, boolean generated) {
    this.id = id;
    this.generated = generated;
  }

  @Override
  public boolean equals(@Nullable Object that) {
    if (that instanceof TupleTag) {
      return this.id.equals(((TupleTag<?>) that).id);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return "Tag<" + id + ">";
  }
}
