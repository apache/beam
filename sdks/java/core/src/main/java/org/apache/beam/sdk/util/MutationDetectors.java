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
import com.google.common.io.BaseEncoding;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuralByteArray;

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
     * The structural value from {@link #possiblyModifiedObject}.
     * It will be used during every call to
     * {@link #verifyUnmodified}, which could be called many times throughout the lifetime of this
     * {@link CodedValueMutationDetector}.
     */
    private final Object originalStructuralValue;

    /**
     * Create a mutation detector for the provided {@code value}, using the provided {@link Coder}
     * for cloning and checking serialized forms for equality.
     */
    public CodedValueMutationDetector(T value, Coder<T> coder) throws CoderException {
      this.coder = coder;
      this.originalStructuralValue = coder.structuralValue(value);
      this.possiblyModifiedObject = value;

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
      Object newStructuralValue = coder.structuralValue(possiblyModifiedObject);
      if (originalStructuralValue.equals(newStructuralValue)) {
          return;
      }
      illegalMutation(originalStructuralValue, newStructuralValue);
    }
    private String encodeToBase64 (StructuralByteArray stream) {
        return BaseEncoding.base64Url().omitPadding().encode(stream.getValue());
    }
    private void illegalMutation(Object previousStructuralValue,
                                   Object newStructuralValue) throws CoderException {
      String previousValue;
      String newValue;
      if (previousStructuralValue instanceof StructuralByteArray) {
          previousValue = encodeToBase64((StructuralByteArray) previousStructuralValue);
          newValue = encodeToBase64((StructuralByteArray) newStructuralValue);
        } else {
          previousValue = CoderUtils.encodeToBase64(coder, (T) previousStructuralValue);
          newValue = CoderUtils.encodeToBase64(coder, (T) newStructuralValue);
      }
      throw new IllegalMutationException(
              String.format("Value %s mutated illegally, new value was %s."
                              + " Encoding was %s, now %s.",
                      previousStructuralValue, newStructuralValue,
                      previousValue,
                      newValue),
              previousStructuralValue, newStructuralValue);
      }

    @Override
    public void close() {
      verifyUnmodified();
    }
  }
}
