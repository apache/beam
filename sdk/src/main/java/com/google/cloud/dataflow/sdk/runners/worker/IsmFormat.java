/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter;
import com.google.cloud.dataflow.sdk.util.VarInt;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An Ism file is a prefix encoded key value file with a bloom filter and an index to
 * enable lookups.
 *
 * <p>An Ism file is composed of these high level sections (in order):
 * <ul>
 *   <li>data block</li>
 *   <li>bloom filter (See {@link ScalableBloomFilter} for details on encoding format)</li>
 *   <li>index</li>
 *   <li>footer (See {@link Footer} for details on encoding format)</li>
 * </ul>
 *
 * <p>The data block is composed of multiple copies of the following:
 * <ul>
 *   <li>key prefix (See {@link KeyPrefix} for details on encoding format)</li>
 *   <li>unshared key bytes</li>
 *   <li>value bytes</li>
 * </ul>
 *
 * <p>The index is composed of {@code N} copies of the following:
 * <ul>
 *   <li>key prefix (See {@link KeyPrefix} for details on encoding format)</li>
 *   <li>unshared key bytes</li>
 *   <li>byte offset to key prefix in data block (variable length long coding)</li>
 * </ul>
 */
class IsmFormat {
  /**
   * The prefix used before each key which contains the number of shared and unshared
   * bytes from the previous key that was read. The key prefix along with the previous key
   * and the unshared key bytes allows one to construct the current key by doing the following
   * {@code currentKey = previousKey[0 : sharedBytes] + read(unsharedBytes)}.
   *
   * <p>The key prefix is encoded as:
   * <ul>
   *   <li>number of shared key bytes (variable length integer coding)</li>
   *   <li>number of unshared key bytes (variable length integer coding)</li>
   * </ul>
   */
  static class KeyPrefix {
    private final int sharedKeySize;
    private final int unsharedKeySize;

    KeyPrefix(int sharedBytes, int unsharedBytes) {
      this.sharedKeySize = sharedBytes;
      this.unsharedKeySize = unsharedBytes;
    }

    public int getSharedKeySize() {
      return sharedKeySize;
    }

    public int getUnsharedKeySize() {
      return unsharedKeySize;
    }

    @Override
    public int hashCode() {
      return Objects.hash(sharedKeySize, unsharedKeySize);
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof KeyPrefix)) {
        return false;
      }
      KeyPrefix keyPrefix = (KeyPrefix) other;
      return sharedKeySize == keyPrefix.sharedKeySize
          && unsharedKeySize == keyPrefix.unsharedKeySize;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("sharedKeySize", sharedKeySize)
          .add("unsharedKeySize", unsharedKeySize)
          .toString();
    }
  }

  /** A {@link Coder} for {@link KeyPrefix}. */
  static final class KeyPrefixCoder extends AtomicCoder<KeyPrefix> {
    private static final KeyPrefixCoder INSTANCE = new KeyPrefixCoder();

    @JsonCreator
    public static KeyPrefixCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(KeyPrefix value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      VarInt.encode(value.sharedKeySize, outStream);
      VarInt.encode(value.unsharedKeySize, outStream);
    }

    @Override
    public KeyPrefix decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      return new KeyPrefix(VarInt.decodeInt(inStream), VarInt.decodeInt(inStream));
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(KeyPrefix value, Coder.Context context) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(KeyPrefix value, Coder.Context context)
        throws Exception {
      Preconditions.checkNotNull(value);
      return VarInt.getLength(value.sharedKeySize) + VarInt.getLength(value.unsharedKeySize);
    }
  }

  /**
   * The footer stores the relevant information required to locate the index and bloom filter.
   * It also stores a version byte and the number of keys stored.
   *
   * <p>The footer is encoded as the value containing:
   * <ul>
   *   <li>start of bloom filter offset (big endian long coding)</li>
   *   <li>start of index position offset (big endian long coding)</li>
   *   <li>number of keys in file (big endian long coding)</li>
   *   <li>0x01 (version key as a single byte)</li>
   * </ul>
   */
  static class Footer {
    static final int LONG_BYTES = 8;
    static final long FIXED_LENGTH = 3 * LONG_BYTES + 1;
    static final byte VERSION = 1;

    private final long indexPosition;
    private final long bloomFilterPosition;
    private final long numberOfKeys;

    Footer(long indexPosition, long bloomFilterPosition, long numberOfKeys) {
      this.indexPosition = indexPosition;
      this.bloomFilterPosition = bloomFilterPosition;
      this.numberOfKeys = numberOfKeys;
    }

    public long getIndexPosition() {
      return indexPosition;
    }

    public long getBloomFilterPosition() {
      return bloomFilterPosition;
    }

    public long getNumberOfKeys() {
      return numberOfKeys;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof Footer)) {
        return false;
      }
      Footer footer = (Footer) other;
      return indexPosition == footer.indexPosition
          && bloomFilterPosition == footer.bloomFilterPosition
          && numberOfKeys == footer.numberOfKeys;
    }

    @Override
    public int hashCode() {
      return Objects.hash(indexPosition, bloomFilterPosition, numberOfKeys);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("version", 1)
          .add("indexPosition", indexPosition)
          .add("bloomFilterPosition", bloomFilterPosition)
          .add("numberOfKeys", numberOfKeys)
          .toString();
    }
  }

  /** A {@link Coder} for {@link Footer}. */
  static final class FooterCoder extends AtomicCoder<Footer> {
    private static final FooterCoder INSTANCE = new FooterCoder();

    @JsonCreator
    public static FooterCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(Footer value, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      DataOutputStream dataOut = new DataOutputStream(outStream);
      dataOut.writeLong(value.indexPosition);
      dataOut.writeLong(value.bloomFilterPosition);
      dataOut.writeLong(value.numberOfKeys);
      dataOut.write(Footer.VERSION);
    }

    @Override
    public Footer decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      DataInputStream dataIn = new DataInputStream(inStream);
      Footer footer = new Footer(dataIn.readLong(), dataIn.readLong(), dataIn.readLong());
      int version = dataIn.read();
      if (version != Footer.VERSION) {
        throw new IOException("Unknown version " + version + ". "
            + "Only version 0x01 is currently supported.");
      }
      return footer;
    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Footer value, Coder.Context context) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(Footer value, Coder.Context context)
        throws Exception {
      return Footer.FIXED_LENGTH;
    }
  }
}

