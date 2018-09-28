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
package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Wraps an exception along with an input value; this is the element type of failure collections
 * returned by single message transforms configured to catch exceptions.
 *
 * @param <T> type of the wrapped input value that caused an exception to be raised
 */
@AutoValue
public abstract class Failure<T> implements Serializable {
  public static <T> Failure<T> of(Exception exception, T value) {
    return new AutoValue_Failure<>(exception, value);
  }

  public abstract Exception exception();

  public abstract T value();

  /**
   * Internal class for collecting tuple tags associated with collections of {@link Exception}
   * classes that should route to them. Also contains helper methods to simplify implementation of
   * the {@code WithFailures} nested classes of {@link MapElements}, {@link FlatMapElements}, etc.
   */
  @AutoValue
  abstract static class TaggedExceptionsList<T> implements Serializable {
    abstract ImmutableList<TupleTag<Failure<T>>> tags();

    abstract ImmutableList<List<Class<?>>> exceptionLists();

    static <T> TaggedExceptionsList<T> empty() {
      return new AutoValue_Failure_TaggedExceptionsList<>(ImmutableList.of(), ImmutableList.of());
    }

    /**
     * Return a new {@link TaggedExceptionsList} that has all the tags and exceptions of this {@link
     * TaggedExceptionsList} plus a new element representing the arguments passed in here.
     */
    TaggedExceptionsList<T> and(
        TupleTag<Failure<T>> tag, Class<?> exceptionToCatch, Class<?>[] additionalExceptions) {
      final ImmutableList<TupleTag<Failure<T>>> newTags =
          ImmutableList.<TupleTag<Failure<T>>>builder().addAll(tags()).add(tag).build();
      final ImmutableList<List<Class<?>>> newExceptionLists =
          ImmutableList.<List<Class<?>>>builder()
              .addAll(exceptionLists())
              .add(
                  ImmutableList.copyOf(ObjectArrays.concat(exceptionToCatch, additionalExceptions)))
              .build();
      return new AutoValue_Failure_TaggedExceptionsList<>(newTags, newExceptionLists);
    }

    /** Return the internal typed list of tags as an untyped {@link TupleTagList}. */
    TupleTagList tupleTagList() {
      TupleTagList l = TupleTagList.empty();
      for (TupleTag<?> tag : tags()) {
        l = l.and(tag);
      }
      return l;
    }

    /**
     * Check the registered exception classes to see if the exception passed in here matches. If it
     * does, wrap the exception and value together in a {@link Failure} and send to the output
     * receiver. If not, rethrow so processing stops on the unexpected failure.
     */
    void outputOrRethrow(Exception e, T value, MultiOutputReceiver receiver) throws Exception {
      for (int i = 0; i < tags().size(); i++) {
        for (Class<?> cls : exceptionLists().get(i)) {
          if (cls.isInstance(e)) {
            receiver.get(tags().get(i)).output(Failure.of(e, value));
            return;
          }
        }
      }
      throw e;
    }

    /**
     * Set appropriate coders on all the failure collections in the given {@link PCollectionTuple}.
     */
    PCollectionTuple applyFailureCoders(PCollectionTuple pcs) {
      final SerializableCoder<Failure<T>> failureCoder =
          SerializableCoder.of(new TypeDescriptor<Failure<T>>() {});
      for (TupleTag<Failure<T>> tag : tags()) {
        pcs.get(tag).setCoder(failureCoder);
      }
      return pcs;
    }
  }
}
