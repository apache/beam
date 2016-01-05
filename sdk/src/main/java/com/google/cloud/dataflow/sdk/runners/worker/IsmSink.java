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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.Footer;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.FooterCoder;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.KeyPrefix;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.KeyPrefixCoder;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.MimeTypes;
import com.google.cloud.dataflow.sdk.util.RandomAccessData;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter.ScalableBloomFilterCoder;
import com.google.cloud.dataflow.sdk.util.VarInt;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.io.CountingOutputStream;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * A {@link Sink} that writes Ism files. The coder provided is used to encode each key value
 * record. See {@link IsmFormat} for encoded format details.
 *
 * @param <K> the type of the keys written to the sink
 * @param <V> the type of the values written to the sink
 */
public class IsmSink<K, V> extends Sink<WindowedValue<KV<K, V>>> {
  private final String filename;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  IsmSink(String filename, Coder<K> keyCoder, Coder<V> valueCoder) {
    this.filename = filename;
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public SinkWriter<WindowedValue<KV<K, V>>> writer() throws IOException {
    return new IsmSinkWriter(IOChannelUtils.create(filename, MimeTypes.BINARY));
  }

  private class IsmSinkWriter implements SinkWriter<WindowedValue<KV<K, V>>> {
    private static final long MAX_BLOCK_SIZE = 1024 * 1024;

    private final CountingOutputStream out;
    private final RandomAccessData indexOut;
    private RandomAccessData lastKeyBytes;
    private RandomAccessData currentKeyBytes;
    private RandomAccessData lastIndexKeyBytes;
    private long lastIndexedPosition;
    private long numberOfKeysWritten;
    private final ScalableBloomFilter.Builder bloomFilterBuilder;

    /**
     * Creates an IsmSinkWriter for the given channel.
     */
    private IsmSinkWriter(WritableByteChannel channel) {
      checkNotNull(channel);
      out = new CountingOutputStream(Channels.newOutputStream(channel));
      indexOut = new RandomAccessData();
      lastKeyBytes = new RandomAccessData();
      currentKeyBytes = new RandomAccessData();
      lastIndexKeyBytes = new RandomAccessData();
      bloomFilterBuilder = ScalableBloomFilter.builder();
    }

    @Override
    public long add(WindowedValue<KV<K, V>> windowedValue) throws IOException {
      // The windowed portion of the value is ignored.
      KV<K, V> value = windowedValue.getValue();

      long currentPosition = out.getCount();
      // Marshal the key, compute the common prefix length
      keyCoder.encode(value.getKey(), currentKeyBytes.asOutputStream(), Context.OUTER);
      int keySize = currentKeyBytes.size();
      int sharedKeySize = commonPrefixLength(lastKeyBytes, currentKeyBytes);

      // Put key-value mapping record into block buffer
      int unsharedKeySize = keySize - sharedKeySize;
      KeyPrefix keyPrefix = new KeyPrefix(sharedKeySize, unsharedKeySize);
      KeyPrefixCoder.of().encode(keyPrefix, out, Context.NESTED);
      currentKeyBytes.writeTo(out, sharedKeySize, unsharedKeySize);
      valueCoder.encode(value.getValue(), out, Context.NESTED);

      // If we have emitted enough bytes to add another entry into the index.
      if (lastIndexedPosition + MAX_BLOCK_SIZE < out.getCount()) {
        int sharedIndexKeySize = commonPrefixLength(lastIndexKeyBytes, currentKeyBytes);
        int unsharedIndexKeySize = keySize - sharedIndexKeySize;
        KeyPrefix indexKeyPrefix = new KeyPrefix(sharedIndexKeySize, unsharedIndexKeySize);
        KeyPrefixCoder.of().encode(indexKeyPrefix, indexOut.asOutputStream(), Context.NESTED);
        currentKeyBytes.writeTo(
            indexOut.asOutputStream(), sharedIndexKeySize, unsharedIndexKeySize);
        VarInt.encode(currentPosition, indexOut.asOutputStream());
        lastIndexKeyBytes.resetTo(0);
        currentKeyBytes.writeTo(lastIndexKeyBytes.asOutputStream(), 0, currentKeyBytes.size());
      }

      // Update the bloom filter
      bloomFilterBuilder.put(currentKeyBytes.array(), 0, currentKeyBytes.size());

      // Swap the current key and the previous key, resetting the previous key to be re-used.
      RandomAccessData temp = lastKeyBytes;
      lastKeyBytes = currentKeyBytes;
      currentKeyBytes = temp;
      currentKeyBytes.resetTo(0);

      numberOfKeysWritten += 1;
      return out.getCount() - currentPosition;
    }

    /**
     * Compute the length of the common prefix of the previous key and the given key
     * and perform a key order check. We check that the currently being inserted key
     * is strictly greater than the previous key.
     */
    private int commonPrefixLength(
        RandomAccessData prevKeyBytes, RandomAccessData currentKeyBytes) {
      byte[] prevKey = prevKeyBytes.array();
      byte[] currentKey = currentKeyBytes.array();
      int minBytesLen = Math.min(prevKeyBytes.size(), currentKeyBytes.size());
      for (int i = 0; i < minBytesLen; i++) {
        // unsigned comparison
        int b1 = prevKey[i] & 0xFF;
        int b2 = currentKey[i] & 0xFF;
        if (b1 > b2) {
          throw new IllegalArgumentException(IsmSinkWriter.class.getSimpleName()
              + " expects keys to be written in strictly increasing order but was given "
              + prevKeyBytes + " as the previous key and " + currentKeyBytes
              + " as the current key. Expected " + b1 + " <= " + b2 + " at position " + i + ".");
        }
        if (b1 != b2) {
          return i;
        }
      }
      if (prevKeyBytes.size() >= currentKeyBytes.size()) {
        throw new IllegalArgumentException(IsmSinkWriter.class.getSimpleName()
            + " expects keys to be written in strictly increasing order but was given "
            + prevKeyBytes + " as the previous key and " + currentKeyBytes
            + " as the current key. Expected length of previous key " + prevKeyBytes.size()
            + " <= " + currentKeyBytes.size() + " to current key.");
      }
      return minBytesLen;
    }

    /**
     * Completes the construction of the Ism file.
     *
     * @throws IOException if an underlying write fails
     */
    private void finish() throws IOException {
      long startOfBloomFilter = out.getCount();
      ScalableBloomFilterCoder.of().encode(bloomFilterBuilder.build(), out, Context.NESTED);
      long startOfIndex = out.getCount();
      indexOut.writeTo(out, 0, indexOut.size());
      FooterCoder.of().encode(new Footer(startOfIndex, startOfBloomFilter, numberOfKeysWritten),
          out, Coder.Context.OUTER);
    }

    @Override
    public void close() throws IOException {
      finish();
      out.close();
    }
  }
}

