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

package com.google.cloud.dataflow.sdk.util.common.worker;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;

/** Unit tests for {@link CachingShuffleBatchReader}. */
@RunWith(JUnit4.class)
public final class CachingShuffleBatchReaderTest {

  private final ShuffleBatchReader.Batch testBatch =
      new ShuffleBatchReader.Batch(new ArrayList<ShuffleEntry>(), null);

  @Test
  public void readerShouldCacheReads() throws IOException {
    ShuffleBatchReader base = mock(ShuffleBatchReader.class);
    CachingShuffleBatchReader reader = new CachingShuffleBatchReader(base);
    when(base.read(null, null)).thenReturn(testBatch);
    // N.B. We need to capture the result of reader.read() in order to ensure
    // that there's a strong reference to it, preventing it from being
    // collected.  Not that this should be an issue in tests, but it's good to
    // be solid.
    ShuffleBatchReader.Batch read = reader.read(null, null);
    assertThat(read, equalTo(testBatch));
    assertThat(reader.read(null, null), equalTo(testBatch));
    assertThat(reader.read(null, null), equalTo(testBatch));
    assertThat(reader.read(null, null), equalTo(testBatch));
    assertThat(reader.read(null, null), equalTo(testBatch));
    verify(base, times(1)).read(null, null);
  }

  @Test
  public void readerShouldNotCacheExceptions() throws IOException {
    ShuffleBatchReader base = mock(ShuffleBatchReader.class);
    CachingShuffleBatchReader reader = new CachingShuffleBatchReader(base);
    when(base.read(null, null))
        .thenThrow(new IOException("test"))
        .thenReturn(testBatch);
    try {
      reader.read(null, null);
      fail("expected an IOException");
    } catch (IOException e) {
      // Nothing to do -- exception is expected.
    }
    assertThat(reader.read(null, null), equalTo(testBatch));
    verify(base, times(2)).read(null, null);
  }

  @Test
  public void readerShouldRereadClearedBatches() throws IOException {
    ShuffleBatchReader base = mock(ShuffleBatchReader.class);
    CachingShuffleBatchReader reader = new CachingShuffleBatchReader(base);
    when(base.read(null, null)).thenReturn(testBatch);
    ShuffleBatchReader.Batch read = reader.read(null, null);
    assertThat(read, equalTo(testBatch));
    verify(base, times(1)).read(null, null);
    CachingShuffleBatchReader.BatchRange range =
        new CachingShuffleBatchReader.BatchRange(null, null);
    CachingShuffleBatchReader.RangeReadReference ref =
        reader.cache.get(range);
    assertThat(ref, notNullValue());
    ref.clear();
    read = reader.read(null, null);
    assertThat(read, equalTo(testBatch));
    verify(base, times(2)).read(null, null);
  }
}
