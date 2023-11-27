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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import org.apache.beam.sdk.util.BufferedElementCountingOutputStream;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

/**
 * An abstract base class with functionality for assembling a {@link Coder} for a class that
 * implements {@code Iterable}.
 *
 * <p>To complete a subclass, implement the {@link #decodeToIterable} method. This superclass will
 * decode the elements in the input stream into a {@link List} and then pass them to that method to
 * be converted into the appropriate iterable type. Note that this means the input iterables must
 * fit into memory.
 *
 * <p>The format of this coder is as follows:
 *
 * <ul>
 *   <li>If the input {@link Iterable} has a known and finite size, then the size is written to the
 *       output stream in big endian format, followed by all of the encoded elements.
 *   <li>If the input {@link Iterable} is not known to have a finite size, then each element of the
 *       input is preceded by {@code true} encoded as a byte (indicating "more data") followed by
 *       the encoded element, and terminated by {@code false} encoded as a byte.
 * </ul>
 *
 * @param <T> the type of the elements of the {@code Iterable}s being transcoded
 * @param <IterableT> the type of the Iterables being transcoded
 */
public abstract class IterableLikeCoder<T, IterableT extends Iterable<T>>
    extends StructuredCoder<IterableT> {
  public Coder<T> getElemCoder() {
    return elementCoder;
  }

  /**
   * Builds an instance of {@code IterableT}, this coder's associated {@link Iterable}-like subtype,
   * from a list of decoded elements.
   *
   * <p>Override {@link #decodeToIterable(List, long, InputStream)} if you need access to the
   * terminator value and the {@link InputStream}.
   */
  protected abstract IterableT decodeToIterable(List<T> decodedElements);

  /**
   * Builds an instance of {@code IterableT}, this coder's associated {@link Iterable}-like subtype,
   * from a list of decoded elements with the {@link InputStream} at the position where this coder
   * detected the end of the stream.
   */
  protected IterableT decodeToIterable(
      List<T> decodedElements, long terminatorValue, InputStream in) throws IOException {
    throw new IllegalStateException(
        String.format(
            "%s does not support non zero terminator values. Received stream with terminator %s.",
            iterableName, terminatorValue));
  }

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  private final Coder<T> elementCoder;
  private final String iterableName;

  protected IterableLikeCoder(Coder<T> elementCoder, String iterableName) {
    checkArgument(elementCoder != null, "element Coder for IterableLikeCoder must not be null");
    checkArgument(iterableName != null, "iterable name for IterableLikeCoder must not be null");
    this.elementCoder = elementCoder;
    this.iterableName = iterableName;
  }

  @Override
  public void encode(IterableT iterable, OutputStream outStream)
      throws IOException, CoderException {
    if (iterable == null) {
      throw new CoderException("cannot encode a null " + iterableName);
    }
    if (iterable instanceof Collection) {
      // We can know the size of the Iterable.  Use an encoding with a
      // leading size field, followed by that many elements.
      Collection<T> collection = (Collection<T>) iterable;
      BitConverters.writeBigEndianInt(collection.size(), outStream);
      for (T elem : collection) {
        elementCoder.encode(elem, outStream);
      }
    } else {
      // We don't know the size without traversing it so use a fixed size buffer
      // and encode as many elements as possible into it before outputting the size followed
      // by the elements.
      BitConverters.writeBigEndianInt(-1, outStream);
      BufferedElementCountingOutputStream countingOutputStream =
          new BufferedElementCountingOutputStream(outStream);
      for (T elem : iterable) {
        countingOutputStream.markElementStart();
        elementCoder.encode(elem, countingOutputStream);
      }
      countingOutputStream.finish();
    }
  }

  @Override
  public IterableT decode(InputStream inStream) throws IOException, CoderException {
    int size = BitConverters.readBigEndianInt(inStream);
    if (size >= 0) {
      List<T> elements = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        elements.add(elementCoder.decode(inStream));
      }
      return decodeToIterable(elements);
    }
    List<T> elements = new ArrayList<>();
    // We don't know the size a priori.  Check if we're done with
    // each block of elements.
    long count = VarInt.decodeLong(inStream);
    while (count > 0L) {
      elements.add(elementCoder.decode(inStream));
      --count;
      if (count == 0L) {
        count = VarInt.decodeLong(inStream);
      }
    }
    if (count == 0) {
      return decodeToIterable(elements);
    } else {
      return decodeToIterable(elements, count, inStream);
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(elementCoder);
  }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always. Encoding is not deterministic for the general {@link
   *     Iterable} case, as it depends upon the type of iterable. This may allow two objects to
   *     compare as equal while the encoding differs.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this, "IterableLikeCoder can not guarantee deterministic ordering.");
  }

  /**
   * {@inheritDoc}
   *
   * @return {@code true} if the iterable is of a known class that supports lazy counting of byte
   *     size, since that requires minimal extra computation.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(IterableT iterable) {
    return iterable instanceof ElementByteSizeObservableIterable;
  }

  @Override
  public void registerByteSizeObserver(IterableT iterable, ElementByteSizeObserver observer)
      throws Exception {
    if (iterable == null) {
      throw new CoderException("cannot encode a null Iterable");
    }

    if (iterable instanceof ElementByteSizeObservableIterable) {
      observer.setLazy();
      ElementByteSizeObservableIterable<?, ?> observableIterable =
          (ElementByteSizeObservableIterable<?, ?>) iterable;
      observableIterable.addObserver(
          new IteratorObserver(observer, iterable instanceof Collection));
    } else {
      if (iterable instanceof Collection) {
        // We can know the size of the Iterable.  Use an encoding with a
        // leading size field, followed by that many elements.
        Collection<T> collection = (Collection<T>) iterable;
        observer.update(4L);
        for (T elem : collection) {
          elementCoder.registerByteSizeObserver(elem, observer);
        }
      } else {
        // TODO: (https://github.com/apache/beam/issues/18169) Update to use an accurate count
        // depending on size and count,
        // currently we are under estimating the size by up to 10 bytes per block of data since we
        // are not encoding the count prefix which occurs at most once per 64k of data and is up to
        // 10 bytes long. Since we include the total count we can upper bound the underestimate
        // to be 10 / 65536 ~= 0.0153% of the actual size.
        observer.update(4L);
        long count = 0;
        for (T elem : iterable) {
          count += 1;
          elementCoder.registerByteSizeObserver(elem, observer);
        }
        if (count > 0) {
          // Update the length based upon the number of counted elements, this helps
          // eliminate the case where all the elements are encoded in the first block and
          // it is quite short (e.g. Long.MAX_VALUE nulls encoded with VoidCoder).
          observer.update(VarInt.getLength(count));
        }
        // Update with the terminator byte.
        observer.update(1L);
      }
    }
  }

  /**
   * An observer that gets notified when an observable iterator returns a new value. This observer
   * just notifies an outerObserver about this event. Additionally, the outerObserver is notified
   * about additional separators that are transparently added by this coder.
   */
  private static class IteratorObserver implements Observer {
    private final ElementByteSizeObserver outerObserver;
    private final boolean countable;

    public IteratorObserver(ElementByteSizeObserver outerObserver, boolean countable) {
      this.outerObserver = outerObserver;
      this.countable = countable;

      if (countable) {
        // Additional 4 bytes are due to size.
        outerObserver.update(4L);
      } else {
        // Additional 5 bytes are due to size = -1 (4 bytes) and
        // hasNext = false (1 byte).
        outerObserver.update(5L);
      }
    }

    @Override
    public void update(Observable obs, Object obj) {
      if (!(obj instanceof Long)) {
        throw new AssertionError("unexpected parameter object");
      }

      if (countable) {
        outerObserver.update(obs, obj);
      } else {
        // Additional 1 byte is due to hasNext = true flag.
        outerObserver.update(obs, 1 + (long) obj);
      }
    }
  }
}
