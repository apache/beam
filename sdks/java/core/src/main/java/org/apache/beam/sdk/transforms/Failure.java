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

  /** Wraps a TupleTag together with the exceptions types that should be routed to it. */
  @AutoValue
  abstract static class TaggedExceptions<T> implements Serializable {
    static <T> TaggedExceptions<T> of(TupleTag<Failure<T>> tag, List<Class<?>> exceptionsToCatch) {
      return new AutoValue_Failure_TaggedExceptions<>(tag, exceptionsToCatch);
    }

    static <T> TaggedExceptions<T> of(
        TupleTag<Failure<T>> tag, Class exceptionToCatch, Class<?>[] additionalExceptions) {
      return of(
          tag, ImmutableList.copyOf(ObjectArrays.concat(exceptionToCatch, additionalExceptions)));
    }

    abstract TupleTag<Failure<T>> tag();

    abstract List<Class<?>> exceptionsToCatch();
  }

  @AutoValue
  abstract static class TaggedExceptionsList<T> implements Serializable {
    abstract ImmutableList<TupleTag<Failure<T>>> tags();

    abstract ImmutableList<List<Class<?>>> exceptionLists();

    static <T> TaggedExceptionsList<T> empty() {
      return new AutoValue_Failure_TaggedExceptionsList<>(ImmutableList.of(), ImmutableList.of());
    }

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

    TupleTagList tupleTagList() {
      TupleTagList l = TupleTagList.empty();
      for (TupleTag<?> tag : tags()) {
        l = l.and(tag);
      }
      return l;
    }

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
