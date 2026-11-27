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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Observable;
import java.util.Observer;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link IterableCoder}. */
@RunWith(JUnit4.class)
public class IterableCoderTest {

  private static final Coder<Iterable<Integer>> TEST_CODER = IterableCoder.of(VarIntCoder.of());

  private static final List<Iterable<Integer>> TEST_VALUES =
      Arrays.asList(
          Collections.emptyList(),
          Collections.singletonList(13),
          Arrays.asList(1, 2, 3, 4),
          new ArrayList<>(Arrays.asList(7, 6, 5)));

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() throws Exception {
    CoderProperties.coderSerializable(ListCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testDecodeEncodeContentsInSameOrder() throws Exception {
    for (Iterable<Integer> value : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeContentsInSameOrder(TEST_CODER, value);
    }
  }

  @Test
  public void testCoderSerializable() throws Exception {
    CoderProperties.coderSerializable(TEST_CODER);
  }

  /**
   * Generated data to check that the wire format has not changed. To regenerate, see {@link
   * org.apache.beam.sdk.coders.PrintBase64Encodings}.
   */
  private static final List<String> TEST_ENCODINGS =
      Arrays.asList("AAAAAA", "AAAAAQ0", "AAAABAECAwQ", "AAAAAwcGBQ");

  @Test
  public void testWireFormatEncode() throws Exception {
    CoderProperties.coderEncodesBase64(TEST_CODER, TEST_VALUES, TEST_ENCODINGS);
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void encodeNullThrowsCoderException() throws Exception {
    thrown.expect(CoderException.class);
    thrown.expectMessage("cannot encode a null Iterable");

    CoderUtils.encodeToBase64(TEST_CODER, null);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    TypeDescriptor<Iterable<Integer>> typeDescriptor = new TypeDescriptor<Iterable<Integer>>() {};
    assertThat(TEST_CODER.getEncodedTypeDescriptor(), equalTo(typeDescriptor));
  }

  private static final class FixedSizeIterator extends ElementByteSizeObservableIterator<Integer> {
    private final long[] sizes;
    private int index;

    FixedSizeIterator(long... sizes) {
      this.sizes = sizes;
    }

    @Override
    public boolean hasNext() {
      return index < sizes.length;
    }

    @Override
    public Integer next() {
      if (index >= sizes.length) {
        throw new NoSuchElementException();
      }
      notifyValueReturned(sizes[index]);
      return index++;
    }

    void notifyChanged(Object payload) {
      setChanged();
      notifyObservers(payload);
    }
  }

  private static final class ObservableNonCollection
      extends ElementByteSizeObservableIterable<Integer, FixedSizeIterator> {
    private final long[] sizes;

    ObservableNonCollection(long... sizes) {
      this.sizes = sizes;
    }

    @Override
    protected FixedSizeIterator createIterator() {
      return new FixedSizeIterator(sizes);
    }
  }

  private static final class ObservableCollection
      extends ElementByteSizeObservableIterable<Integer, FixedSizeIterator>
      implements Collection<Integer> {
    private final long[] sizes;

    ObservableCollection(long... sizes) {
      this.sizes = sizes;
    }

    @Override
    protected FixedSizeIterator createIterator() {
      return new FixedSizeIterator(sizes);
    }

    @Override
    public int size() {
      return sizes.length;
    }

    @Override
    public boolean isEmpty() {
      return sizes.length == 0;
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Integer e) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Integer> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }

  private static class RecordingObserver extends ElementByteSizeObserver {
    final List<Long> reported = new ArrayList<>();

    @Override
    protected void reportElementSize(long elementByteSize) {
      reported.add(elementByteSize);
    }
  }

  @Test
  public void testRegisterByteSizeObserverUncountableObservableIterable() throws Exception {
    ObservableNonCollection iterable = new ObservableNonCollection(10L, 200L);
    assertTrue(TEST_CODER.isRegisterByteSizeObserverCheap(iterable));

    RecordingObserver outer = new RecordingObserver();
    TEST_CODER.registerByteSizeObserver(iterable, outer);
    assertTrue(outer.getIsLazy());

    FixedSizeIterator it = iterable.iterator();
    while (it.hasNext()) {
      it.next();
    }
    outer.advance();
    // 5L header (size=-1 + terminator) + (1 + 10) + (1 + 200) = 217L.
    assertThat(outer.reported, equalTo(Collections.singletonList(217L)));
  }

  @Test
  public void testRegisterByteSizeObserverCountableObservableCollection() throws Exception {
    ObservableCollection collection = new ObservableCollection(10L, 200L);
    CollectionCoder<Integer> coder = CollectionCoder.of(VarIntCoder.of());
    assertTrue(coder.isRegisterByteSizeObserverCheap(collection));

    RecordingObserver outer = new RecordingObserver();
    coder.registerByteSizeObserver(collection, outer);
    assertTrue(outer.getIsLazy());

    FixedSizeIterator it = collection.iterator();
    while (it.hasNext()) {
      it.next();
    }
    outer.advance();
    // 4L header (size) + 10 + 200 = 214L.
    assertThat(outer.reported, equalTo(Collections.singletonList(214L)));
  }

  @Test
  public void testIteratorObserverScalingFactorMultipleObserversAndLegacyUpdate() throws Exception {
    ObservableNonCollection iterable = new ObservableNonCollection(10L, 20L);
    RecordingObserver outer = new RecordingObserver();
    outer.setScalingFactor(2.0);
    TEST_CODER.registerByteSizeObserver(iterable, outer);

    List<Object> extraSeen = new ArrayList<>();
    Observer secondObserver = (obs, arg) -> extraSeen.add(arg);
    iterable.addObserver(secondObserver);

    FixedSizeIterator it = iterable.iterator();
    while (it.hasNext()) {
      it.next();
    }
    // Exercise the legacy update(Observable, Object) bridge explicitly with boxed Long.
    it.notifyChanged(Long.valueOf(100L));
    outer.advance();
    // Header: 2 * 5 = 10; elems: 2 * (1 + 10) + 2 * (1 + 20) = 64; legacy: 2 * (1 + 100) = 202.
    assertThat(outer.reported, equalTo(Collections.singletonList(276L)));
    assertThat(extraSeen, equalTo(Arrays.<Object>asList(10L, 20L, 100L)));

    // Exercise legacy update(Observable, Object) on countable ObservableCollection too (zero
    // per-element overhead, Observable reference forwarded).
    ObservableCollection countable = new ObservableCollection(10L);
    List<Object> observedSources = new ArrayList<>();
    RecordingObserver countableOuter =
        new RecordingObserver() {
          @Override
          public void update(Observable obs, Object obj) {
            observedSources.add(obs);
            super.update(obs, obj);
          }
        };
    TEST_CODER.registerByteSizeObserver(countable, countableOuter);
    FixedSizeIterator countableIt = countable.iterator();
    while (countableIt.hasNext()) {
      countableIt.next();
    }
    countableIt.notifyChanged(Long.valueOf(50L));
    countableOuter.advance();
    // 4L header + 10L element + 50L legacy = 64L.
    assertThat(countableOuter.reported, equalTo(Collections.singletonList(64L)));
    assertThat(observedSources, equalTo(Collections.<Object>singletonList(countableIt)));

    // Legacy update(Observable, Object) must reject non-Long payloads.
    ObservableNonCollection badIterable = new ObservableNonCollection();
    TEST_CODER.registerByteSizeObserver(badIterable, new RecordingObserver());
    FixedSizeIterator badIt = badIterable.iterator();
    assertThrows(AssertionError.class, () -> badIt.notifyChanged("not-a-long"));
    assertThrows(AssertionError.class, () -> badIt.notifyChanged(Integer.valueOf(7)));
    assertThrows(AssertionError.class, () -> badIt.notifyChanged(null));
  }
}
