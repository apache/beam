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
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Wraps an exception along with an input value; this is the element type of failure collections
 * returned by single message transforms configured to catch exceptions.
 *
 * @param <InputT> type of the wrapped input value that caused an exception to be raised
 */
@AutoValue
public abstract class Failure<ExceptionT extends Exception, InputT> implements Serializable {
  public static <ExceptionT extends Exception, InputT> Failure<ExceptionT, InputT> of(
      ExceptionT exception, InputT value) {
    return new AutoValue_Failure<>(exception, value);
  }

  public abstract ExceptionT exception();

  public abstract InputT value();

  /** Provides factory {@link #of(Coder, Coder)} for creating coders for {@link Failure}. */
  public static class FailureCoder {

    public static <ExceptionT extends Exception, InputT> Coder<Failure<ExceptionT, InputT>> of(
        Coder<ExceptionT> exceptionCoder, Coder<InputT> valueCoder) {
      return DelegateCoder.of(
          KvCoder.of(exceptionCoder, valueCoder),
          new ToKvCodingFunction<>(),
          new ToFailureCodingFunction<>());
    }

    private static final class ToKvCodingFunction<K extends Exception, V>
        implements DelegateCoder.CodingFunction<Failure<K, V>, KV<K, V>> {
      @Override
      public KV<K, V> apply(Failure<K, V> failure) throws Exception {
        return KV.of(failure.exception(), failure.value());
      }
    }

    private static final class ToFailureCodingFunction<K extends Exception, V>
        implements DelegateCoder.CodingFunction<KV<K, V>, Failure<K, V>> {
      @Override
      public Failure<K, V> apply(KV<K, V> kv) throws Exception {
        return Failure.of(kv.getKey(), kv.getValue());
      }
    }
  }

  /**
   * Internal class for storing a {@code TupleTag<Failure<ExceptionT, InputT>>} and providing the
   * necessary utilities for reflecting the exception type and generating an appropriate coder.
   *
   * <p>Any action that needs to know about the concrete types ExceptionT or InputT lives here.
   */
  @AutoValue
  abstract static class FailureTag<ExceptionT extends Exception, InputT> implements Serializable {
    abstract TupleTag<Failure<ExceptionT, InputT>> tupleTag();

    abstract List<Class<?>> exceptionList();

    abstract Coder<ExceptionT> exceptionCoder();

    static <ExceptionT extends Exception, InputT> FailureTag<ExceptionT, InputT> of(
        TupleTag<Failure<ExceptionT, InputT>> tag, Class<?>[] exceptionsToCatch) {
      final TypeDescriptor excType = getExceptionTypeDescriptor(tag);

      List<Class<?>> exceptionList;
      if (exceptionsToCatch.length > 0) {
        for (Class<?> cls : exceptionsToCatch) {
          if (!excType.getRawType().isAssignableFrom(cls)) {
            throw new IllegalArgumentException(
                "All passed exception classes must be subtypes"
                    + " of type parameter ExceptionT in the given"
                    + " TupleTag<Failure<ExceptionT, InputT>>, but "
                    + cls.toString()
                    + " is not a subtype of "
                    + excType.toString());
          }
        }
        exceptionList = ImmutableList.copyOf(exceptionsToCatch);
      } else {
        exceptionList = ImmutableList.of(excType.getRawType());
      }

      Coder<ExceptionT> exceptionCoder = AvroCoder.of(excType);

      // Exercise the coder early so that we can fail fast in case the given exception type is
      // not valid for AvroCoder.
      try {
        exceptionCoder.decode(null);
      } catch (IOException | NullPointerException e) {
        // We expect NullPointerException here due to null InputStream passed to decode();
        // if we get this far, our type is good and we can move on.
      } catch (RuntimeException e) {
        Throwable cause = e.getCause();
        if (cause instanceof NoSuchMethodException) {
          throw new IllegalArgumentException(
              "Cannot transcode instances of "
                  + excType.toString()
                  + " with AvroCoder; you may want to choose ExceptionT in TupleTag<Failure<ExceptionT, InputT>> to be a "
                  + " superclass that has a no-argument constructor",
              e);
        }
        throw e;
      }
      return new AutoValue_Failure_FailureTag<>(tag, exceptionList, exceptionCoder);
    }

    /** Return true if the passed exception is an instance of one of the configured subclasses. */
    boolean matches(Exception e) {
      for (Class<?> cls : exceptionList()) {
        if (cls.isInstance(e)) {
          return true;
        }
      }
      return false;
    }

    void sendToOutput(Exception e, InputT value, MultiOutputReceiver receiver) {
      receiver.get(tupleTag()).output(Failure.of((ExceptionT) e, value));
    }

    void applyFailureCoder(Coder<InputT> valueCoder, PCollectionTuple pcs) {
      pcs.get(tupleTag()).setCoder(FailureCoder.of(exceptionCoder(), valueCoder));
    }

    static <ExceptionT extends Exception, InputT> TypeDescriptor getExceptionTypeDescriptor(
        TupleTag<Failure<ExceptionT, InputT>> tag) {
      ParameterizedType tagType =
          (ParameterizedType)
              TypeDescriptor.of(tag.getClass()).getSupertype(TupleTag.class).getType();
      ParameterizedType failureType;
      try {
        failureType =
            (ParameterizedType) TypeDescriptor.of(tagType.getActualTypeArguments()[0]).getType();
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(
            "Could not reflect type parameters from the passed"
                + " TupleTag<Failure<ExceptionT, InputT>>, probably because the tag was instantiated"
                + " without ending curly braces; the instantiation should look something like"
                + " `new TupleTag<Failure<IOException, String>>(){}`.",
            e);
      }
      return TypeDescriptor.of(failureType.getActualTypeArguments()[0]);
    }
  }

  /**
   * Internal class for collecting tuple tags associated with collections of {@link Exception}
   * classes that should route to them. Also contains helper methods to simplify implementation of
   * the {@code WithFailures} nested classes of {@link MapElements}, {@link FlatMapElements}, etc.
   */
  @AutoValue
  abstract static class FailureTagList<InputT> implements Serializable {
    abstract List<FailureTag<?, InputT>> failureTags();

    static <T> FailureTagList<T> empty() {
      return new AutoValue_Failure_FailureTagList<>(ImmutableList.of());
    }

    /**
     * Return a new {@link FailureTagList} that has all the tags and exceptions of this {@link
     * FailureTagList} plus a new element representing the arguments passed in here.
     */
    <ExceptionT extends Exception> FailureTagList<InputT> and(
        TupleTag<Failure<ExceptionT, InputT>> tag, Class<?>[] exceptionsToCatch) {
      final ImmutableList<FailureTag<?, InputT>> newFailureTags =
          ImmutableList.<FailureTag<?, InputT>>builder()
              .addAll(failureTags())
              .add(FailureTag.of(tag, exceptionsToCatch))
              .build();
      return new AutoValue_Failure_FailureTagList<>(newFailureTags);
    }

    /** Return the internal typed list of tags as an untyped {@link TupleTagList}. */
    TupleTagList tags() {
      TupleTagList l = TupleTagList.empty();
      for (FailureTag<?, InputT> failureTag : failureTags()) {
        l = l.and(failureTag.tupleTag());
      }
      return l;
    }

    /**
     * Check the registered exception classes to see if the exception passed in here matches. If it
     * does, wrap the exception and value together in a {@link Failure} and send to the output
     * receiver. If not, rethrow so processing stops on the unexpected failure.
     */
    void outputOrRethrow(Exception e, InputT value, MultiOutputReceiver receiver) throws Exception {
      for (FailureTag<?, InputT> failureTag : failureTags()) {
        if (failureTag.matches(e)) {
          failureTag.sendToOutput(e, value, receiver);
          return;
        }
      }
      throw e;
    }

    /**
     * Set appropriate coders on all the failure collections in the given {@link PCollectionTuple}.
     */
    PCollectionTuple applyFailureCoders(Coder<InputT> valueCoder, PCollectionTuple pcs) {
      for (FailureTag<?, InputT> failureTag : failureTags()) {
        failureTag.applyFailureCoder(valueCoder, pcs);
      }
      return pcs;
    }
  }
}
