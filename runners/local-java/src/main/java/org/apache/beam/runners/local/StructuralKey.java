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
package org.apache.beam.runners.local;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A (Key, Coder) pair that uses the structural value of the key (as provided by {@link
 * Coder#structuralValue(Object)}) to perform equality and hashing.
 */
public abstract class StructuralKey<K> {

  private StructuralKey() {
    // Prevents extending outside of this class
  }

  /** Returns the key that this {@link StructuralKey} was created from. */
  public abstract K getKey();

  /** Get the empty {@link StructuralKey}. All instances of the empty key are considered equal. */
  public static StructuralKey<?> empty() {
    return new StructuralKey<Object>() {
      @Override
      public Object getKey() {
        return this;
      }
    };
  }

  /** Create a new Structural Key of the provided key that can be encoded by the provided coder. */
  public static <K> StructuralKey<K> of(K key, Coder<K> coder) {
    try {
      return new CoderStructuralKey<>(coder, key);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Could not encode a key with its provided coder " + coder.getClass().getSimpleName(), e);
    }
  }

  private static class CoderStructuralKey<K> extends StructuralKey<K> {
    private final Coder<K> coder;
    private final Object structuralValue;
    private final byte[] encoded;

    private CoderStructuralKey(Coder<K> coder, K key) throws Exception {
      this.coder = coder;
      this.structuralValue = coder.structuralValue(key);
      this.encoded = CoderUtils.encodeToByteArray(coder, key);
    }

    @Override
    public K getKey() {
      try {
        return CoderUtils.decodeFromByteArray(coder, encoded);
      } catch (CoderException e) {
        throw new IllegalArgumentException(
            "Could not decode Key with coder of type " + coder.getClass().getSimpleName(), e);
      }
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == this) {
        return true;
      }
      if (other instanceof CoderStructuralKey) {
        CoderStructuralKey<?> that = (CoderStructuralKey<?>) other;
        return structuralValue.equals(that.structuralValue);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return structuralValue.hashCode();
    }
  }
}
