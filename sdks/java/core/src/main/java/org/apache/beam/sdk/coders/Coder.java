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
package org.apache.beam.sdk.coders;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CountingOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Coder Coder&lt;T&gt;} defines how to encode and decode values of type {@code T} into
 * byte streams.
 *
 * <p>{@link Coder} instances are serialized during job creation and deserialized before use. This
 * will generally be performed by serializing the object via Java Serialization.
 *
 * <p>{@link Coder} classes for compound types are often composed from coder classes for types
 * contains therein. The composition of {@link Coder} instances into a coder for the compound class
 * is the subject of the {@link CoderProvider} type, which enables automatic generic composition of
 * {@link Coder} classes within the {@link CoderRegistry}. See {@link CoderProvider} and {@link
 * CoderRegistry} for more information about how coders are inferred.
 *
 * <p>All methods of a {@link Coder} are required to be thread safe.
 *
 * @param <T> the type of values being encoded and decoded
 */
public abstract class Coder<T> implements Serializable {
  /**
   * The context in which encoding or decoding is being done.
   *
   * @deprecated To implement a coder, do not use any {@link Context}. Just implement only those
   *     abstract methods which do not accept a {@link Context} and leave the default
   *     implementations for methods accepting a {@link Context}.
   */
  @Deprecated
  @Experimental(Kind.CODER_CONTEXT)
  public static class Context {
    /**
     * The outer context: the value being encoded or decoded takes up the remainder of the
     * record/stream contents.
     */
    public static final Context OUTER = new Context(true);

    /**
     * The nested context: the value being encoded or decoded is (potentially) a part of a larger
     * record/stream contents, and may have other parts encoded or decoded after it.
     */
    public static final Context NESTED = new Context(false);

    /**
     * Whether the encoded or decoded value fills the remainder of the output or input (resp.)
     * record/stream contents. If so, then the size of the decoded value can be determined from the
     * remaining size of the record/stream contents, and so explicit lengths aren't required.
     */
    public final boolean isWholeStream;

    public Context(boolean isWholeStream) {
      this.isWholeStream = isWholeStream;
    }

    public Context nested() {
      return NESTED;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (!(obj instanceof Context)) {
        return false;
      }
      return Objects.equal(isWholeStream, ((Context) obj).isWholeStream);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(isWholeStream);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Context.class)
          .addValue(isWholeStream ? "OUTER" : "NESTED")
          .toString();
    }
  }

  /**
   * Encodes the given value of type {@code T} onto the given output stream.
   *
   * @throws IOException if writing to the {@code OutputStream} fails for some reason
   * @throws CoderException if the value could not be encoded for some reason
   */
  public abstract void encode(T value, OutputStream outStream) throws CoderException, IOException;

  /**
   * Encodes the given value of type {@code T} onto the given output stream in the given context.
   *
   * @throws IOException if writing to the {@code OutputStream} fails for some reason
   * @throws CoderException if the value could not be encoded for some reason
   * @deprecated only implement and call {@link #encode(Object value, OutputStream)}
   */
  @Deprecated
  @Experimental(Kind.CODER_CONTEXT)
  public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    encode(value, outStream);
  }

  /**
   * Decodes a value of type {@code T} from the given input stream in the given context. Returns the
   * decoded value.
   *
   * @throws IOException if reading from the {@code InputStream} fails for some reason
   * @throws CoderException if the value could not be decoded for some reason
   */
  public abstract T decode(InputStream inStream) throws CoderException, IOException;

  /**
   * Decodes a value of type {@code T} from the given input stream in the given context. Returns the
   * decoded value.
   *
   * @throws IOException if reading from the {@code InputStream} fails for some reason
   * @throws CoderException if the value could not be decoded for some reason
   * @deprecated only implement and call {@link #decode(InputStream)}
   */
  @Deprecated
  @Experimental(Kind.CODER_CONTEXT)
  public T decode(InputStream inStream, Context context) throws CoderException, IOException {
    return decode(inStream);
  }

  /**
   * If this is a {@link Coder} for a parameterized type, returns the list of {@link Coder}s being
   * used for each of the parameters in the same order they appear within the parameterized type's
   * type signature. If this cannot be done, or this {@link Coder} does not encode/decode a
   * parameterized type, returns the empty list.
   */
  public abstract List<? extends Coder<?>> getCoderArguments();

  /**
   * Throw {@link NonDeterministicException} if the coding is not deterministic.
   *
   * <p>In order for a {@code Coder} to be considered deterministic, the following must be true:
   *
   * <ul>
   *   <li>two values that compare as equal (via {@code Object.equals()} or {@code
   *       Comparable.compareTo()}, if supported) have the same encoding.
   *   <li>the {@code Coder} always produces a canonical encoding, which is the same for an instance
   *       of an object even if produced on different computers at different times.
   * </ul>
   *
   * @throws Coder.NonDeterministicException if this coder is not deterministic.
   */
  public abstract void verifyDeterministic() throws Coder.NonDeterministicException;

  /**
   * Verifies all of the provided coders are deterministic. If any are not, throws a {@link
   * NonDeterministicException} for the {@code target} {@link Coder}.
   */
  public static void verifyDeterministic(Coder<?> target, String message, Iterable<Coder<?>> coders)
      throws NonDeterministicException {
    for (Coder<?> coder : coders) {
      try {
        coder.verifyDeterministic();
      } catch (NonDeterministicException e) {
        throw new NonDeterministicException(target, message, e);
      }
    }
  }

  /**
   * Verifies all of the provided coders are deterministic. If any are not, throws a {@link
   * NonDeterministicException} for the {@code target} {@link Coder}.
   */
  public static void verifyDeterministic(Coder<?> target, String message, Coder<?>... coders)
      throws NonDeterministicException {
    verifyDeterministic(target, message, Arrays.asList(coders));
  }

  /**
   * Returns {@code true} if this {@link Coder} is injective with respect to {@link Objects#equals}.
   *
   * <p>Whenever the encoded bytes of two values are equal, then the original values are equal
   * according to {@code Objects.equals()}. Note that this is well-defined for {@code null}.
   *
   * <p>This condition is most notably false for arrays. More generally, this condition is false
   * whenever {@code equals()} compares object identity, rather than performing a
   * semantic/structural comparison.
   *
   * <p>By default, returns false.
   */
  public boolean consistentWithEquals() {
    return false;
  }

  /**
   * Returns an object with an {@code Object.equals()} method that represents structural equality on
   * the argument.
   *
   * <p>For any two values {@code x} and {@code y} of type {@code T}, if their encoded bytes are the
   * same, then it must be the case that {@code structuralValue(x).equals(structuralValue(y))}.
   *
   * <p>Most notably:
   *
   * <ul>
   *   <li>The structural value for an array coder should perform a structural comparison of the
   *       contents of the arrays, rather than the default behavior of comparing according to object
   *       identity.
   *   <li>The structural value for a coder accepting {@code null} should be a proper object with an
   *       {@code equals()} method, even if the input value is {@code null}.
   * </ul>
   *
   * <p>See also {@link #consistentWithEquals()}.
   *
   * <p>By default, if this coder is {@link #consistentWithEquals()}, and the value is not null,
   * returns the provided object. Otherwise, encodes the value into a {@code byte[]}, and returns an
   * object that performs array equality on the encoded bytes.
   */
  public Object structuralValue(T value) {
    if (value != null && consistentWithEquals()) {
      return value;
    } else {
      try {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        encode(value, os, Context.OUTER);
        return new StructuralByteArray(os.toByteArray());
      } catch (Exception exn) {
        throw new IllegalArgumentException(
            "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
      }
    }
  }

  /**
   * Returns whether {@link #registerByteSizeObserver} cheap enough to call for every element, that
   * is, if this {@code Coder} can calculate the byte size of the element to be coded in roughly
   * constant time (or lazily).
   *
   * <p>Not intended to be called by user code, but instead by {@link PipelineRunner}
   * implementations.
   *
   * <p>By default, returns false. The default {@link #registerByteSizeObserver} implementation
   * invokes {@link #getEncodedElementByteSize} which requires re-encoding an element unless it is
   * overridden. This is considered expensive.
   */
  public boolean isRegisterByteSizeObserverCheap(T value) {
    return false;
  }

  /**
   * Notifies the {@code ElementByteSizeObserver} about the byte size of the encoded value using
   * this {@code Coder}.
   *
   * <p>Not intended to be called by user code, but instead by {@link PipelineRunner}
   * implementations.
   *
   * <p>By default, this notifies {@code observer} about the byte size of the encoded value using
   * this coder as returned by {@link #getEncodedElementByteSize}.
   */
  public void registerByteSizeObserver(T value, ElementByteSizeObserver observer) throws Exception {
    observer.update(getEncodedElementByteSize(value));
  }

  /** Returns the size in bytes of the encoded value using this coder. */
  protected long getEncodedElementByteSize(T value) throws Exception {
    try (CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream())) {
      encode(value, os);
      return os.getCount();
    } catch (Exception exn) {
      throw new IllegalArgumentException(
          "Unable to encode element '" + value + "' with coder '" + this + "'.", exn);
    }
  }

  /** Returns the {@link TypeDescriptor} for the type encoded. */
  @Experimental(Kind.CODER_TYPE_ENCODING)
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return (TypeDescriptor<T>)
        TypeDescriptor.of(getClass()).resolveType(new TypeDescriptor<T>() {}.getType());
  }

  /**
   * Exception thrown by {@link Coder#verifyDeterministic()} if the encoding is not deterministic,
   * including details of why the encoding is not deterministic.
   */
  public static class NonDeterministicException extends Exception {
    private Coder<?> coder;
    private List<String> reasons;

    public NonDeterministicException(
        Coder<?> coder, String reason, @Nullable NonDeterministicException e) {
      this(coder, Arrays.asList(reason), e);
    }

    public NonDeterministicException(Coder<?> coder, String reason) {
      this(coder, Arrays.asList(reason), null);
    }

    public NonDeterministicException(Coder<?> coder, List<String> reasons) {
      this(coder, reasons, null);
    }

    public NonDeterministicException(
        Coder<?> coder, List<String> reasons, @Nullable NonDeterministicException cause) {
      super(cause);
      checkArgument(reasons.size() > 0, "Reasons must not be empty.");
      this.reasons = reasons;
      this.coder = coder;
    }

    public Iterable<String> getReasons() {
      return reasons;
    }

    @Override
    public String getMessage() {
      String reasonsStr = Joiner.on("\n\t").join(reasons);
      return coder + " is not deterministic because:\n\t" + reasonsStr;
    }
  }
}
