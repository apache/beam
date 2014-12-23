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

import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservableIterable;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Observable;
import java.util.Observer;

/**
 * The base class of Coders for Iterable subclasses.
 *
 * @param <T> the type of the elements of the Iterables being transcoded
 * @param <IT> the type of the Iterables being transcoded
 */
public abstract class IterableLikeCoder<T, IT extends Iterable<T>>
    extends StandardCoder<IT> {

  public Coder<T> getElemCoder() { return elementCoder; }

  /**
   * Builds an instance of the coder's associated {@code Iterable} from a list
   * of decoded elements.  If {@code IT} is a supertype of {@code List<T>}, the
   * derived class implementation is permitted to return {@code decodedElements}
   * directly.
   */
  protected abstract IT decodeToIterable(List<T> decodedElements);

  /////////////////////////////////////////////////////////////////////////////
  // Internal operations below here.

  private final Coder<T> elementCoder;

  /**
   * Returns the first element in this iterable-like if it is non-empty,
   * otherwise returns {@code null}.
   */
  protected static <T, IT extends Iterable<T>>
      List<Object> getInstanceComponentsHelper(
          IT exampleValue) {
    for (T value : exampleValue) {
      return Arrays.<Object>asList(value);
    }
    return null;
  }

  protected IterableLikeCoder(Coder<T> elementCoder) {
    this.elementCoder = elementCoder;
  }

  @Override
  public void encode(IT iterable, OutputStream outStream, Context context)
      throws IOException, CoderException  {
    if (iterable == null) {
      throw new CoderException("cannot encode a null Iterable");
    }
    Context nestedContext = context.nested();
    DataOutputStream dataOutStream = new DataOutputStream(outStream);
    if (iterable instanceof Collection) {
      // We can know the size of the Iterable.  Use an encoding with a
      // leading size field, followed by that many elements.
      Collection<T> collection = (Collection<T>) iterable;
      dataOutStream.writeInt(collection.size());
      for (T elem : collection) {
        elementCoder.encode(elem, dataOutStream, nestedContext);
      }
    } else {
      // We don't know the size without traversing it.  So use a
      // "hasNext" sentinel before each element.
      // TODO: Don't use the sentinel if context.isWholeStream.
      dataOutStream.writeInt(-1);
      for (T elem : iterable) {
        dataOutStream.writeBoolean(true);
        elementCoder.encode(elem, dataOutStream, nestedContext);
      }
      dataOutStream.writeBoolean(false);
    }
    // Make sure all our output gets pushed to the underlying outStream.
    dataOutStream.flush();
  }

  @Override
  public IT decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    Context nestedContext = context.nested();
    DataInputStream dataInStream = new DataInputStream(inStream);
    int size = dataInStream.readInt();
    if (size >= 0) {
      List<T> elements = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        elements.add(elementCoder.decode(dataInStream, nestedContext));
      }
      return decodeToIterable(elements);
    } else {
      // We don't know the size a priori.  Check if we're done with
      // each element.
      List<T> elements = new ArrayList<>();
      while (dataInStream.readBoolean()) {
        elements.add(elementCoder.decode(dataInStream, nestedContext));
      }
      return decodeToIterable(elements);
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Arrays.asList(elementCoder);
  }

  /**
   * Encoding is not deterministic for the general Iterable case, as it depends
   * upon the type of iterable. This may allow two objects to compare as equal
   * while the encoding differs.
   */
  @Override
  public boolean isDeterministic() {
    return false;
  }

  /**
   * Returns whether iterable can use lazy counting, since that
   * requires minimal extra computation.
   */
  @Override
  public boolean isRegisterByteSizeObserverCheap(IT iterable, Context context) {
    return iterable instanceof ElementByteSizeObservableIterable;
  }

  /**
   * Notifies ElementByteSizeObserver about the byte size of the
   * encoded value using this coder.
   */
  @Override
  public void registerByteSizeObserver(
      IT iterable, ElementByteSizeObserver observer, Context context)
      throws Exception {
    if (iterable == null) {
      throw new CoderException("cannot encode a null Iterable");
    }
    Context nestedContext = context.nested();

    if (iterable instanceof ElementByteSizeObservableIterable) {
      observer.setLazy();
      ElementByteSizeObservableIterable<?, ?> observableIT =
          (ElementByteSizeObservableIterable) iterable;
      observableIT.addObserver(
          new IteratorObserver(observer, iterable instanceof Collection));
    } else {
      if (iterable instanceof Collection) {
        // We can know the size of the Iterable.  Use an encoding with a
        // leading size field, followed by that many elements.
        Collection<T> collection = (Collection<T>) iterable;
        observer.update(4L);
        for (T elem : collection) {
          elementCoder.registerByteSizeObserver(elem, observer, nestedContext);
        }
      } else {
        // We don't know the size without traversing it.  So use a
        // "hasNext" sentinel before each element.
        // TODO: Don't use the sentinel if context.isWholeStream.
        observer.update(4L);
        for (T elem : iterable) {
          observer.update(1L);
          elementCoder.registerByteSizeObserver(elem, observer, nestedContext);
        }
        observer.update(1L);
      }
    }
  }

  /**
   * An observer that gets notified when an observable iterator
   * returns a new value. This observer just notifies an outerObserver
   * about this event. Additionally, the outerObserver is notified
   * about additional separators that are transparently added by this
   * coder.
   */
  private class IteratorObserver implements Observer {
    private final ElementByteSizeObserver outerObserver;
    private final boolean countable;

    public IteratorObserver(ElementByteSizeObserver outerObserver,
                            boolean countable) {
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
