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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A {@link Coder Coder&lt;T&gt;} defines how to encode and decode values of type {@code T} into
 * byte streams.
 *
 * <p>{@link Coder} instances are serialized during job creation and deserialized
 * before use, via JSON serialization. See {@link SerializableCoder} for an example of a
 * {@link Coder} that adds a custom field to
 * the {@link Coder} serialization. It provides a constructor annotated with
 * {@link com.fasterxml.jackson.annotation.JsonCreator}, which is a factory method used when
 * deserializing a {@link Coder} instance.
 *
 * <p>{@link Coder} classes for compound types are often composed from coder classes for types
 * contains therein. The composition of {@link Coder} instances into a coder for the compound
 * class is the subject of the {@link CoderFactory} type, which enables automatic generic
 * composition of {@link Coder} classes within the {@link CoderRegistry}. With particular
 * static methods on a compound {@link Coder} class, a {@link CoderFactory} can be automatically
 * inferred. See {@link KvCoder} for an example of a simple compound {@link Coder} that supports
 * automatic composition in the {@link CoderRegistry}.
 *
 * <p>The binary format of a {@link Coder} is identified by {@link #getEncodingId()}; be sure to
 * understand the requirements for evolving coder formats.
 *
 * <p>All methods of a {@link Coder} are required to be thread safe.
 *
 * @param <T> the type of the values being transcoded
 */
public interface Coder<T> extends Serializable {
  /** The context in which encoding or decoding is being done. */
  public static class Context {
    /**
     * The outer context: the value being encoded or decoded takes
     * up the remainder of the record/stream contents.
     */
    public static final Context OUTER = new Context(true);

    /**
     * The nested context: the value being encoded or decoded is
     * (potentially) a part of a larger record/stream contents, and
     * may have other parts encoded or decoded after it.
     */
    public static final Context NESTED = new Context(false);

    /**
     * Whether the encoded or decoded value fills the remainder of the
     * output or input (resp.) record/stream contents.  If so, then
     * the size of the decoded value can be determined from the
     * remaining size of the record/stream contents, and so explicit
     * lengths aren't required.
     */
    public final boolean isWholeStream;

    public Context(boolean isWholeStream) {
      this.isWholeStream = isWholeStream;
    }

    public Context nested() {
      return NESTED;
    }

    @Override
    public boolean equals(Object obj) {
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
          .addValue(isWholeStream ? "OUTER" : "NESTED").toString();
    }
  }

  /**
   * Encodes the given value of type {@code T} onto the given output stream
   * in the given context.
   *
   * @throws IOException if writing to the {@code OutputStream} fails
   * for some reason
   * @throws CoderException if the value could not be encoded for some reason
   */
  public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException;

  /**
   * Decodes a value of type {@code T} from the given input stream in
   * the given context.  Returns the decoded value.
   *
   * @throws IOException if reading from the {@code InputStream} fails
   * for some reason
   * @throws CoderException if the value could not be decoded for some reason
   */
  public T decode(InputStream inStream, Context context)
      throws CoderException, IOException;

  /**
   * If this is a {@code Coder} for a parameterized type, returns the
   * list of {@code Coder}s being used for each of the parameters, or
   * returns {@code null} if this cannot be done or this is not a
   * parameterized type.
   */
  public List<? extends Coder<?>> getCoderArguments();

  /**
   * Returns the {@link CloudObject} that represents this {@code Coder}.
   */
  public CloudObject asCloudObject();

  /**
   * Throw {@link NonDeterministicException} if the coding is not deterministic.
   *
   * <p>In order for a {@code Coder} to be considered deterministic,
   * the following must be true:
   * <ul>
   *   <li>two values that compare as equal (via {@code Object.equals()}
   *       or {@code Comparable.compareTo()}, if supported) have the same
   *       encoding.
   *   <li>the {@code Coder} always produces a canonical encoding, which is the
   *       same for an instance of an object even if produced on different
   *       computers at different times.
   * </ul>
   *
   * @throws Coder.NonDeterministicException if this coder is not deterministic.
   */
  public void verifyDeterministic() throws Coder.NonDeterministicException;

  /**
   * Returns {@code true} if this {@link Coder} is injective with respect to {@link Objects#equals}.
   *
   * <p>Whenever the encoded bytes of two values are equal, then the original values are equal
   * according to {@code Objects.equals()}. Note that this is well-defined for {@code null}.
   *
   * <p>This condition is most notably false for arrays. More generally, this condition is false
   * whenever {@code equals()} compares object identity, rather than performing a
   * semantic/structural comparison.
   */
  public boolean consistentWithEquals();

  /**
   * Returns an object with an {@code Object.equals()} method that represents structural equality
   * on the argument.
   *
   * <p>For any two values {@code x} and {@code y} of type {@code T}, if their encoded bytes are the
   * same, then it must be the case that {@code structuralValue(x).equals(@code structuralValue(y)}.
   *
   * <p>Most notably:
   * <ul>
   *   <li>The structural value for an array coder should perform a structural comparison of the
   *   contents of the arrays, rather than the default behavior of comparing according to object
   *   identity.
   *   <li>The structural value for a coder accepting {@code null} should be a proper object with
   *   an {@code equals()} method, even if the input value is {@code null}.
   * </ul>
   *
   * <p>See also {@link #consistentWithEquals()}.
   */
  public Object structuralValue(T value) throws Exception;

  /**
   * Returns whether {@link #registerByteSizeObserver} cheap enough to
   * call for every element, that is, if this {@code Coder} can
   * calculate the byte size of the element to be coded in roughly
   * constant time (or lazily).
   *
   * <p>Not intended to be called by user code, but instead by
   * {@link org.apache.beam.sdk.runners.PipelineRunner}
   * implementations.
   */
  public boolean isRegisterByteSizeObserverCheap(T value, Context context);

  /**
   * Notifies the {@code ElementByteSizeObserver} about the byte size
   * of the encoded value using this {@code Coder}.
   *
   * <p>Not intended to be called by user code, but instead by
   * {@link org.apache.beam.sdk.runners.PipelineRunner}
   * implementations.
   */
  public void registerByteSizeObserver(
      T value, ElementByteSizeObserver observer, Context context)
      throws Exception;

  /**
   * An identifier for the binary format written by {@link #encode}.
   *
   * <p>This value, along with the fully qualified class name, forms an identifier for the
   * binary format of this coder. Whenever this value changes, the new encoding is considered
   * incompatible with the prior format: It is presumed that the prior version of the coder will
   * be unable to correctly read the new format and the new version of the coder will be unable to
   * correctly read the old format.
   *
   * <p>If the format is changed in a backwards-compatible way (the Coder can still accept data from
   * the prior format), such as by adding optional fields to a Protocol Buffer or Avro definition,
   * and you want Dataflow to understand that the new coder is compatible with the prior coder,
   * this value must remain unchanged. It is then the responsibility of {@link #decode} to correctly
   * read data from the prior format.
   */
  @Experimental(Kind.CODER_ENCODING_ID)
  public String getEncodingId();

  /**
   * A collection of encodings supported by {@link #decode} in addition to the encoding
   * from {@link #getEncodingId()} (which is assumed supported).
   *
   * <p><i>This information is not currently used for any purpose</i>. It is descriptive only,
   * and this method is subject to change.
   *
   * @see #getEncodingId()
   */
  @Experimental(Kind.CODER_ENCODING_ID)
  public Collection<String> getAllowedEncodings();

  /**
   * Exception thrown by {@link Coder#verifyDeterministic()} if the encoding is
   * not deterministic, including details of why the encoding is not deterministic.
   */
  public static class NonDeterministicException extends Throwable {
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
        Coder<?> coder,
        List<String> reasons,
        @Nullable NonDeterministicException cause) {
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
      return String.format("%s is not deterministic because:\n  %s",
          coder, Joiner.on("\n  ").join(reasons));
    }
  }
}
