/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;

/**
 * A {@code Coder<T>} defines how to encode and decode values of type {@code T} into byte streams.
 *
 * <p> All methods of a {@code Coder<T>} are required to be thread safe.
 *
 * <p> {@code Coder}s are serialized during job creation and deserialized
 * before use, via JSON serialization.
 *
 * <p> See {@link SerializableCoder} for an example of a {@code Coder} that adds
 * a custom field to the {@code Coder} serialization. It provides a
 * constructor annotated with {@link
 * com.fasterxml.jackson.annotation.JsonCreator}, which is a factory method
 * used when deserializing a {@code Coder} instance.
 *
 * <p> See {@link KvCoder} for an example of a nested {@code Coder} type.
 *
 * @param <T> the type of the values being transcoded
 */
public interface Coder<T> extends Serializable {
  /** The context in which encoding or decoding is being done. */
  public static class Context {
    /**
     * The outer context.  The value being encoded or decoded takes
     * up the remainder of the whole record/stream contents.
     */
    public static final Context OUTER = new Context(true);

    /**
     * The nested context.  The value being encoded or decoded is
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
   * Returns true if the coding is deterministic.
   *
   * <p> In order for a {@code Coder} to be considered deterministic,
   * the following must be true:
   * <ul>
   *   <li>two values which compare as equal (via {@code Object.equals()}
   *       or {@code Comparable.compareTo()}, if supported), have the same
   *       encoding.
   *   <li>the {@code Coder} always produces a canonical encoding, which is the
   *       same for an instance of an object even if produced on different
   *       computers at different times.
   * </ul>
   */
  public boolean isDeterministic();

  /**
   * Returns whether {@link #registerByteSizeObserver} cheap enough to
   * call for every element, that is, if this {@code Coder} can
   * calculate the byte size of the element to be coded in roughly
   * constant time (or lazily).
   *
   * <p> Not intended to be called by user code, but instead by
   * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner}
   * implementations.
   */
  public boolean isRegisterByteSizeObserverCheap(T value, Context context);

  /**
   * Notifies the {@code ElementByteSizeObserver} about the byte size
   * of the encoded value using this {@code Coder}.
   *
   * <p> Not intended to be called by user code, but instead by
   * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner}
   * implementations.
   */
  public void registerByteSizeObserver(
      T value, ElementByteSizeObserver observer, Context context)
      throws Exception;
}
