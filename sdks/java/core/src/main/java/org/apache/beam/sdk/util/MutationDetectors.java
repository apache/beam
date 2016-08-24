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
package org.apache.beam.sdk.util;

import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

/**
 * Static methods for creating and working with {@link MutationDetector}.
 */
public class MutationDetectors {

  private MutationDetectors() {}

  /**
     * Creates a new {@code MutationDetector} for the provided {@code value} that uses the provided
     * {@link Coder} to perform deep copies and comparisons by serializing and deserializing values.
     *
     * <p>It is permissible for {@code value} to be {@code null}. Since {@code null} is immutable,
     * the mutation check will always succeed.
     */
  public static <T> MutationDetector forValueWithCoder(T value, Coder<T> coder)
      throws CoderException {
    if (value == null) {
      return noopMutationDetector();
    } else {
      return new CodedValueMutationDetector<>(value, coder);
    }
  }

  /**
   * Creates a new {@code MutationDetector} that always succeeds.
   *
   * <p>This is useful, for example, for providing a very efficient mutation detector for a value
   * which is already immutable by design.
   */
  public static MutationDetector noopMutationDetector() {
    return new NoopMutationDetector();
  }

  /**
   * A {@link MutationDetector} for {@code null}, which is immutable.
   */
  private static class NoopMutationDetector implements MutationDetector {

    @Override
    public void verifyUnmodified() { }

    @Override
    public void close() { }
  }

  /**
   * Given a value of type {@code T} and a {@link Coder} for that type, provides facilities to save
   * check that the value has not changed.
   *
   * @param <T> the type of values checked for mutation
   */
  private static class CodedValueMutationDetector<T> implements MutationDetector {

    private final Coder<T> coder;

    /**
     * A saved pointer to an in-memory value provided upon construction, which we will check for
     * forbidden mutations.
     */
    private final T possiblyModifiedObject;

    /**
     * A saved encoded copy of the same value as {@link #possiblyModifiedObject}. Naturally, it
     * will not change if {@link #possiblyModifiedObject} is mutated.
     */
    private final byte[] encodedOriginalObject;

    /**
     * The object decoded from {@link #encodedOriginalObject}. It will be used during every call to
     * {@link #verifyUnmodified}, which could be called many times throughout the lifetime of this
     * {@link CodedValueMutationDetector}.
     */
    private final T clonedOriginalObject;

    /**
     * Create a mutation detector for the provided {@code value}, using the provided {@link Coder}
     * for cloning and checking serialized forms for equality.
     */
    public CodedValueMutationDetector(T value, Coder<T> coder) throws CoderException {
      this.coder = coder;
      this.possiblyModifiedObject = value;
      this.encodedOriginalObject = CoderUtils.encodeToByteArray(coder, value);
      this.clonedOriginalObject = CoderUtils.decodeFromByteArray(coder, encodedOriginalObject);
    }

    @Override
    public void verifyUnmodified() {
      try {
        verifyUnmodifiedThrowingCheckedExceptions();
      } catch (CoderException exn) {
        throw new RuntimeException(exn);
      }
    }

    private void verifyUnmodifiedThrowingCheckedExceptions() throws CoderException {
      // If either object believes they are equal, we trust that and short-circuit deeper checks.
      if (Objects.equals(possiblyModifiedObject, clonedOriginalObject)
          || Objects.equals(clonedOriginalObject, possiblyModifiedObject)) {
        return;
      }

      // Since retainedObject is in general an instance of a subclass of T, when it is cloned to
      // clonedObject using a Coder<T>, the two will generally be equivalent viewed as a T, but in
      // general neither retainedObject.equals(clonedObject) nor clonedObject.equals(retainedObject)
      // will hold.
      //
      // For example, CoderUtils.clone(IterableCoder<Integer>, IterableSubclass<Integer>) will
      // produce an ArrayList<Integer> with the same contents as the IterableSubclass, but the
      // latter will quite reasonably not consider itself equivalent to an ArrayList (and vice
      // versa).
      //
      // To enable a reasonable comparison, we clone retainedObject again here, converting it to
      // the same sort of T that the Coder<T> output when it created clonedObject.
      T clonedPossiblyModifiedObject = CoderUtils.clone(coder, possiblyModifiedObject);

      // If deepEquals() then we trust the equals implementation.
      // This deliberately allows fields to escape this check.
      if (Objects.deepEquals(clonedPossiblyModifiedObject, clonedOriginalObject)) {
        return;
      }

      // If not deepEquals(), the class may just have a poor equals() implementation.
      // So we next try checking their serialized forms. We re-serialize instead of checking
      // encodedObject, because the Coder may treat it differently.
      //
      // For example, an unbounded Iterable will be encoded in an unbounded way, but decoded into an
      // ArrayList, which will then be re-encoded in a bounded format. So we really do need to
      // encode-decode-encode retainedObject.
      if (Arrays.equals(
          CoderUtils.encodeToByteArray(coder, clonedOriginalObject),
          CoderUtils.encodeToByteArray(coder, clonedPossiblyModifiedObject))) {
        return;
      }

      // If we got here, then they are not deepEquals() and do not have deepEquals() encodings.
      // Even if there is some conceptual sense in which the objects are equivalent, it has not
      // been adequately expressed in code.
      illegalMutation(clonedOriginalObject, clonedPossiblyModifiedObject);
    }

    private void illegalMutation(T previousValue, T newValue) throws CoderException {
      throw new IllegalMutationException(
          String.format("Value %s mutated illegally, new value was %s."
              + " Encoding was %s, now %s.",
              previousValue, newValue,
              CoderUtils.encodeToBase64(coder, previousValue),
              CoderUtils.encodeToBase64(coder, newValue)),
          previousValue, newValue);
    }

    @Override
    public void close() {
      verifyUnmodified();
    }
  }
}
