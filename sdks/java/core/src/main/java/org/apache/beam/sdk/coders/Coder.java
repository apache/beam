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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CountingOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Coder} defines how values of type {@code T} are encoded into bytes and decoded back into
 * objects.
 *
 * <p>Coders are used by Beam to serialize data when it is transferred between transforms,
 * persisted, or sent across process boundaries.
 *
 * <p>The {@link #encode(Object, OutputStream, Context)} and {@link #decode(InputStream, Context)}
 * methods must be consistent: values encoded by {@code encode} must be correctly reconstructed by
 * {@code decode}.
 *
 * <p>The {@link Context} parameter specifies whether the value is encoded as a top-level element or
 * as part of a larger structure. This affects whether additional information (such as length
 * prefixes) is required to ensure that encoded values can be unambiguously decoded.
 *
 * <p>For example:
 *
 * <ul>
 *   <li>In {@link Context#OUTER}, the value may consume the entire stream and does not require
 *       explicit length encoding.
 *   <li>In {@link Context#NESTED}, the value is part of a larger structure and must be encoded in a
 *       self-delimiting way.
 * </ul>
 *
 * <p>Coder implementations must be:
 *
 * <ul>
 *   <li>Deterministic (when required by the pipeline)
 *   <li>Thread-safe
 *   <li>Consistent between encode and decode
 * </ul>
 *
 * @param <T> the type of values handled by this {@link Coder}
 * 
 * <p>The behavior of encoding and decoding depends on the {@link Context}.
 *
 * <ul>
 *   <li>In {@link Context#OUTER}, the value consumes the remainder of the stream.
 *   <li>In {@link Context#NESTED}, the value is part of a larger structure and must be
 *       self-delimiting so that subsequent values can be correctly decoded.
 * </ul>
 *
 * <p>See {@link CoderProperties} for utilities to test coder correctness and consistency.
 */
public abstract class Coder<T> implements Serializable {
  /**
   * Represents the context in which encoding or decoding is performed.
   *
   * <p>The {@link Context} determines whether the value being encoded or decoded is part of a
   * larger structure or is the outermost value in the stream.
   *
   * <p>This distinction is important because some coders need to include additional information
   * (such as length prefixes) when values are nested inside other structures, but can omit them
   * when operating on the outermost level.
   *
   * <p>There are two standard contexts:
   *
   * <ul>
   *   <li>{@link #OUTER} – Indicates that the value occupies the remainder of the input or output
   *       stream. In this case, coders may omit length information because the boundaries are
   *       implicitly known.
   *   <li>{@link #NESTED} – Indicates that the value is encoded as part of a larger structure.
   *       Coders must ensure that the encoded value is self-delimiting, typically by including
   *       length prefixes or other boundary markers.
   * </ul>
   *
   * <p>For example:
   *
   * <ul>
   *   <li>When encoding a top-level element in a file → use {@code OUTER}
   *   <li>When encoding elements inside a collection (e.g., list, KV, etc.) → use {@code NESTED}
   * </ul>
   *
   * <p>Correct usage of {@link Context} ensures that encoded data can be safely and correctly
   * decoded without ambiguity.
   *
   * <p><b>Note:</b> Most coder implementations do not need to manually manage {@link Context}. They
   * should delegate to component coders with the appropriate context when encoding nested
   * structures.
   *
   * @deprecated This class is deprecated and will be removed in future release. 
   *  Use {@link Coder.Context} alternatives provided by the Beam SDk instead.
   */
  @Deprecated
  public static class Context {
    /**
     * The outer context indicates that the value being encoded or decoded occupies the remainder of
     * the input or output stream.
     *
     * <p>In this context, the boundaries of the value are implicitly known, so coders do not need
     * to include additional length information or delimiters when encoding.
     *
     * <p>This is typically used for top-level values, such as elements written directly to a file
     * or stream.
     */
    public static final Context OUTER = new Context(true);

    /**
     * The nested context indicates that the value being encoded or decoded is part of a larger
     * structure and does not occupy the entire stream.
     *
     * <p>In this context, coders must ensure that the encoded value is self-delimiting, typically
     * by including length prefixes or other boundary markers, so that subsequent data in the stream
     * can be correctly decoded.
     *
     * <p>This is commonly used when encoding elements inside collections, key-value pairs, or other
     * composite data structures.
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
   * Encodes the given value of type {@code T} onto the provided output stream.
   *
   * <p>The encoding must be deterministic and consistent with {@link #decode}, such that values
   * written by this method can be correctly reconstructed.
   *
   * <p>The {@link Context} determines how the value should be encoded:
   *
   * <ul>
   *   <li>In {@link Context#OUTER}, the value is written as a top-level element and may omit length
   *       prefixes or delimiters since it consumes the remainder of the stream.
   *   <li>In {@link Context#NESTED}, the value is part of a larger structure and must include
   *       sufficient boundary information (such as length prefixes) to allow correct decoding of
   *       subsequent data.
   * </ul>
   *
   * <p>Implementations must ensure that the encoding is unambiguous and that multiple encoded
   * values can be safely concatenated and decoded in sequence.
   *
   * @param value the value to encode
   * @param outStream the output stream to write the encoded bytes to
   * @throws IOException if writing to the stream fails
   * @throws CoderException if the value cannot be encoded
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
  public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    encode(value, outStream);
  }

  /**
   * Decodes a value of type {@code T} from the given input stream.
   *
   * <p>The decoding must be consistent with {@link #encode}, such that values encoded by this coder
   * can be correctly reconstructed.
   *
   * <p>When multiple values are encoded sequentially in a stream, implementations must read exactly
   * the bytes corresponding to a single encoded value and no more. This ensures that subsequent
   * values in the stream can be decoded correctly.
   *
   * <p>Depending on how the value was encoded, the implementation may rely on implicit boundaries
   * (for outer context) or explicit boundary information such as length prefixes (for nested
   * context).
   *
   * <p>Implementations must ensure that decoding is unambiguous and does not consume bytes beyond
   * the encoded representation of the value.
   *
   * @param inStream the input stream to read the encoded value from
   * @return the decoded value
   * @throws IOException if reading from the stream fails
   * @throws CoderException if the value cannot be decoded
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

  public static <T> long getEncodedElementByteSizeUsingCoder(Coder<T> target, T value)
      throws Exception {
    return target.getEncodedElementByteSize(value);
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
