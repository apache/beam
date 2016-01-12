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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.Footer;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.FooterCoder;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.KeyPrefix;
import com.google.cloud.dataflow.sdk.runners.worker.IsmFormat.KeyPrefixCoder;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.RandomAccessData;
import com.google.cloud.dataflow.sdk.util.RandomAccessData.RandomAccessDataCoder;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter.ScalableBloomFilterCoder;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

/**
 * A {@link NativeReader} that reads Ism files. The coder provided is used to encode each key value
 * record. See {@link IsmFormat} for encoded format details.
 *
 * @param <K> the type of the keys written to the sink
 * @param <V> the type of the values written to the sink
 */
public class IsmReader<K, V> extends NativeReader<KV<K, V>> {
  private final String filename;
  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  /** Lazily initialized on first read. */
  private long length;
  private Footer footer;

  /** Lazily initialized on first keyed read. */
  private ImmutableSortedMap<RandomAccessData, Long> index;
  ScalableBloomFilter bloomFilter;

  IsmReader(final String filename, Coder<K> keyCoder, Coder<V> valueCoder) {
    this.filename = filename;
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public LazyIsmReaderIterator iterator() throws IOException {
    return new LazyIsmReaderIterator();
  }

  /**
   * Returns a {@code KV<K, V>} pair for the given {@code K} or null if {@code K} is not
   * present within this Ism file.
   */
  public KV<K, V> get(K k) throws IOException {
    try (SeekableByteChannel inChannel = initializeForKeyedRead()) {
      RandomAccessData keyBytes = new RandomAccessData();

      // Encode the requested key
      keyCoder.encode(k, keyBytes.asOutputStream(), Context.OUTER);

      // If the Bloom filter says we don't have the key, we have nothing further to do.
      if (!bloomFilterMightContain(keyBytes)) {
        return null;
      }

      // Find the index record which is less than or equal to the passed in key.
      Entry<RandomAccessData, Long> entry = index.floorEntry(keyBytes);

      // If no indexed entry is less than or equal to the passed in key then we start
      // at the beginning of the file.
      if (entry == null) {
        entry = new AbstractMap.SimpleEntry<>(new RandomAccessData(), 0L);
      }

      // Reposition the stream to the data block that should contain the key.
      inChannel.position(entry.getValue());

      // Seek through the data block till we find a key that matches or a greater key.
      try (IsmReaderIterator<RandomAccessData, V> iterator =
              new IsmReaderIterator<>(
                  inChannel,
                  entry.getKey(),
                  RandomAccessDataCoder.of(),
                  valueCoder,
                  footer.getBloomFilterPosition())) {
        while (iterator.hasNext()) {
          long startPosition = inChannel.position();
          KV<RandomAccessData, V> next = iterator.next();
          int comparison = RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR.compare(
              next.getKey(), keyBytes);
          // If the current key is greater then the requested key, this Ism file does not contain
          // the record.
          if (comparison > 0) {
            return null;
          } else if (comparison == 0) {
            notifyElementRead(inChannel.position() - startPosition);
            return KV.of(k, next.getValue());
          }
        }
      }
      // We hit the end of the file, therefore this Ism file does not contain the key.
      return null;
    }
  }

  // Overridable by tests to get around the bloom filter not containing any values.
  @VisibleForTesting
  boolean bloomFilterMightContain(RandomAccessData keyBytes) {
    return bloomFilter.mightContain(keyBytes.array(), 0, keyBytes.size());
  }

  /**
   * Initialize this Ism reader by reading the footer.
   */
  private synchronized void initializeFooter(SeekableByteChannel in) throws IOException {
    if (footer != null) {
      return;
    }
    this.length = in.size();
    in.position(length - Footer.FIXED_LENGTH);
    this.footer = FooterCoder.of().decode(Channels.newInputStream(in), Context.OUTER);
    in.position(0L);
  }

  /**
   * A {@link NativeReaderIterator
   * Reader.ReaderIterator} which initializes its input stream lazily.
   */
  private class LazyIsmReaderIterator extends LegacyReaderIterator<KV<K, V>> {
    private IsmReaderIterator<K, V> delegate;
    private SeekableByteChannel inChannel;

    @Override
    public boolean hasNext() throws IOException {
      return getDelegate().hasNext();
    }

    @Override
    public KV<K, V> next() throws IOException, NoSuchElementException {
      long startPosition = getChannel().position();
      KV<K, V> rval = getDelegate().next();
      notifyElementRead(getChannel().position() - startPosition);
      return rval;
    }

    @Override
    public void close() throws IOException {
      inChannel.close();
    }

    /**
     * Return a reader, caching the creation on the first call.
     */
    private IsmReaderIterator<K, V> getDelegate() throws IOException {
      if (delegate == null) {
        inChannel = getChannel();
        initializeFooter(inChannel);
        delegate = new IsmReaderIterator<>(inChannel, new RandomAccessData(),
            keyCoder, valueCoder, footer.getBloomFilterPosition());
      }
      return delegate;
    }

    /**
     * Return a connection, caching the creation on the first call.
     */
    private SeekableByteChannel getChannel() throws IOException {
      if (inChannel == null) {
        inChannel = openConnection(filename);
      }
      return inChannel;
    }
  }

  /**
   * A {@link NativeReaderIterator
   * Reader.ReaderIterator} for Ism formatted files which returns a sequence of
   * {@code KV<K, V>}'s read from a {@link SeekableByteChannel}.
   */
  private static class IsmReaderIterator<K, V> extends LegacyReaderIterator<KV<K, V>> {
    private final SeekableByteChannel inChannel;
    private final InputStream inStream;
    private final RandomAccessData currentKeyBytes;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final long readLimit;

    /**
     * Start an initialized reader that will start from the given key. This reader iterator does
     * not own the channel and the caller must ensure that it is closed.
     */
    public IsmReaderIterator(SeekableByteChannel unownedChannel,
        RandomAccessData currentKeyBytes, Coder<K> keyCoder, Coder<V> valueCoder, long readLimit)
            throws IOException {
      checkNotNull(unownedChannel);
      checkNotNull(currentKeyBytes);
      checkNotNull(keyCoder);
      checkNotNull(valueCoder);
      checkArgument(readLimit >= 0L);
      this.inChannel = unownedChannel;
      this.inStream = Channels.newInputStream(unownedChannel);

      // Copy the key since the IsmReaderIterator mutates the key.
      this.currentKeyBytes = new RandomAccessData(currentKeyBytes.size());
      currentKeyBytes.writeTo(this.currentKeyBytes.asOutputStream(), 0, currentKeyBytes.size());

      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.readLimit = readLimit;
    }

    @Override
    public boolean hasNext() throws IOException {
      if (inChannel.position() > readLimit) {
        throw new IllegalStateException("Read past end of stream");
      }
      return inChannel.position() < readLimit;
    }

    @Override
    public KV<K, V> next() throws IOException, NoSuchElementException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      KeyPrefix keyPrefix = KeyPrefixCoder.of().decode(inStream, Context.NESTED);
      int totalKeyLength = keyPrefix.getSharedKeySize() + keyPrefix.getUnsharedKeySize();
      // currentKey = prevKey[0 : sharedKeySize] + read(unsharedKeySize)
      currentKeyBytes.readFrom(
          inStream,
          keyPrefix.getSharedKeySize() /* start to overwrite the previous key at sharedKeySize */,
          keyPrefix.getUnsharedKeySize() /* read unsharedKeySize bytes from the stream */);
      K key = keyCoder.decode(currentKeyBytes.asInputStream(0, totalKeyLength), Context.OUTER);
      V value = valueCoder.decode(inStream, Context.NESTED);
      return KV.of(key, value);
    }
  }

  /**
   * Initializes the footer, Bloom filter and index if they have not yet been initialized.
   * Returns a {@link SeekableByteChannel} at an arbitrary position within the stream.
   * Callers should re-position the channel to their desired location.
   */
  private SeekableByteChannel initializeForKeyedRead() throws IOException {
    SeekableByteChannel inChannel = openConnection(filename);
    if (index != null) {
      checkState(footer != null, "Footer expected to have been initialized.");
      checkState(bloomFilter != null, "Bloom filter expected to have been initialized.");
      return inChannel;
    }
    checkState(bloomFilter == null, "Bloom filter not expected to have been initialized.");

    initializeFooter(inChannel);

    // Set the position to where the bloom filter is and read it in.
    inChannel.position(footer.getBloomFilterPosition());
    bloomFilter = ScalableBloomFilterCoder.of().decode(
        Channels.newInputStream(inChannel), Context.NESTED);

    // The index follows the bloom filter directly, so we do not need to do a seek here.
    // This is an optimization.
    @SuppressWarnings("resource")
    LegacyReaderIterator<KV<RandomAccessData, Long>> iterator =
        new IsmReaderIterator<RandomAccessData, Long>(
            inChannel,
            new RandomAccessData(),
            RandomAccessDataCoder.of(),
            VarLongCoder.of(),
            length - Footer.FIXED_LENGTH);
    ImmutableSortedMap.Builder<RandomAccessData, Long> builder =
        ImmutableSortedMap.orderedBy(RandomAccessData.UNSIGNED_LEXICOGRAPHICAL_COMPARATOR);

    // Read the index into memory.
    while (iterator.hasNext()) {
      KV<RandomAccessData, Long> next = iterator.next();
      builder.put(next.getKey(), next.getValue());
    }
    index = builder.build();
    return inChannel;
  }

  /**
   * Returns a {@link SeekableByteChannel} for the given {@code filename}.
   */
  private static SeekableByteChannel openConnection(String filename) throws IOException {
    ReadableByteChannel channel = IOChannelUtils.getFactory(filename).open(filename);
    Preconditions.checkArgument(channel instanceof SeekableByteChannel,
        "IsmReader requires a SeekableByteChannel for path %s but received %s.",
        filename, channel);
    return (SeekableByteChannel) channel;
  }
}

