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
package org.apache.beam.runners.spark.io;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.io.Source;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test for {@link SourceRDD.Bounded.ReaderToIteratorAdapter}. */
public class ReaderToIteratorAdapterTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  private static class TestReader extends Source.Reader<Integer> {

    static final int LIMIT = 4;
    static final int START = 1;

    private Integer current = START - 1;
    private boolean closed = false;
    private boolean drained = false;

    boolean isClosed() {
      return closed;
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      checkState(!drained && !closed);
      drained = ++current >= LIMIT;
      return !drained;
    }

    @Override
    public Integer getCurrent() throws NoSuchElementException {
      checkState(!drained && !closed);
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      checkState(!drained && !closed);
      return Instant.now();
    }

    @Override
    public void close() throws IOException {
      checkState(!closed);
      closed = true;
    }

    @Override
    public Source<Integer> getCurrentSource() {
      return null;
    }
  }

  private final TestReader testReader = new TestReader();

  private final SourceRDD.Bounded.ReaderToIteratorAdapter<Integer> readerIterator =
      new SourceRDD.Bounded.ReaderToIteratorAdapter<>(new MetricsContainerImpl(""), testReader);

  private void assertReaderRange(final int start, final int end) {
    for (int i = start; i < end; i++) {
      assertThat(readerIterator.next().getValue(), is(i));
    }
  }

  @Test
  public void testReaderIsClosedAfterDrainage() throws Exception {
    assertReaderRange(TestReader.START, TestReader.LIMIT);

    assertThat(readerIterator.hasNext(), is(false));

    // reader is closed only after hasNext realises there are no more elements
    assertThat(testReader.isClosed(), is(true));
  }

  @Test
  public void testNextWhenDrainedThrows() throws Exception {
    assertReaderRange(TestReader.START, TestReader.LIMIT);

    exception.expect(NoSuchElementException.class);
    readerIterator.next();
  }

  @Test
  public void testHasNextIdempotencyCombo() throws Exception {
    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));

    assertThat(readerIterator.next().getValue(), is(1));

    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));
    assertThat(readerIterator.hasNext(), is(true));

    assertThat(readerIterator.next().getValue(), is(2));
    assertThat(readerIterator.next().getValue(), is(3));

    // drained

    assertThat(readerIterator.hasNext(), is(false));
    assertThat(readerIterator.hasNext(), is(false));

    // no next to give

    exception.expect(NoSuchElementException.class);
    readerIterator.next();
  }
}
