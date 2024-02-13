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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static com.google.api.client.util.Lists.newArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.util.common.Reiterator;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link BatchingShuffleEntryReader}. */
@RunWith(JUnit4.class)
public final class BatchingShuffleEntryReaderTest {
  private static final byte[] KEY = {0xA};
  private static final byte[] SKEY = {0xB};
  private static final byte[] VALUE = {0xC};
  private static final ShufflePosition START_POSITION =
      ByteArrayShufflePosition.of("aaa".getBytes(StandardCharsets.UTF_8));
  private static final ShufflePosition END_POSITION =
      ByteArrayShufflePosition.of("zzz".getBytes(StandardCharsets.UTF_8));
  private static final ShufflePosition NEXT_START_POSITION =
      ByteArrayShufflePosition.of("next".getBytes(StandardCharsets.UTF_8));
  private static final ShufflePosition SECOND_NEXT_START_POSITION =
      ByteArrayShufflePosition.of("next-second".getBytes(StandardCharsets.UTF_8));

  static ShuffleEntry newShuffleEntry(byte[] key, byte[] secondaryKey, byte[] value) {
    return new ShuffleEntry(
        ByteString.copyFrom(key), ByteString.copyFrom(secondaryKey), ByteString.copyFrom(value));
  }

  @Mock private ShuffleBatchReader batchReader;
  private ShuffleEntryReader reader;

  @Before
  public void initMocksAndReader() {
    MockitoAnnotations.initMocks(this);
    reader = new BatchingShuffleEntryReader(batchReader);
  }

  @Test
  public void readerCanRead() throws Exception {
    ShuffleEntry e1 = newShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry e2 = newShuffleEntry(KEY, SKEY, VALUE);
    ArrayList<ShuffleEntry> entries = new ArrayList<>();
    entries.add(e1);
    entries.add(e2);
    when(batchReader.read(START_POSITION, END_POSITION))
        .thenReturn(new ShuffleBatchReader.Batch(entries, null));
    List<ShuffleEntry> results = newArrayList(reader.read(START_POSITION, END_POSITION));
    assertThat(results, contains(e1, e2));
  }

  @Test
  public void readerIteratorCanBeCopied() throws Exception {
    ShuffleEntry e1 = newShuffleEntry(KEY, SKEY, VALUE);
    ShuffleEntry e2 = newShuffleEntry(KEY, SKEY, VALUE);
    ArrayList<ShuffleEntry> entries = new ArrayList<>();
    entries.add(e1);
    entries.add(e2);
    when(batchReader.read(START_POSITION, END_POSITION))
        .thenReturn(new ShuffleBatchReader.Batch(entries, null));
    Reiterator<ShuffleEntry> it = reader.read(START_POSITION, END_POSITION);
    assertThat(it.hasNext(), equalTo(Boolean.TRUE));
    assertThat(it.next(), equalTo(e1));
    Reiterator<ShuffleEntry> copy = it.copy();
    assertThat(it.hasNext(), equalTo(Boolean.TRUE));
    assertThat(it.next(), equalTo(e2));
    assertThat(it.hasNext(), equalTo(Boolean.FALSE));
    assertThat(copy.hasNext(), equalTo(Boolean.TRUE));
    assertThat(copy.next(), equalTo(e2));
    assertThat(copy.hasNext(), equalTo(Boolean.FALSE));
  }

  @Test
  public void readerShouldMergeMultipleBatchResults() throws Exception {
    ShuffleEntry e1 = newShuffleEntry(KEY, SKEY, VALUE);
    List<ShuffleEntry> e1s = Collections.singletonList(e1);
    ShuffleEntry e2 = newShuffleEntry(KEY, SKEY, VALUE);
    List<ShuffleEntry> e2s = Collections.singletonList(e2);
    when(batchReader.read(START_POSITION, END_POSITION))
        .thenReturn(new ShuffleBatchReader.Batch(e1s, NEXT_START_POSITION));
    when(batchReader.read(NEXT_START_POSITION, END_POSITION))
        .thenReturn(new ShuffleBatchReader.Batch(e2s, null));
    List<ShuffleEntry> results = newArrayList(reader.read(START_POSITION, END_POSITION));
    assertThat(results, contains(e1, e2));

    verify(batchReader).read(START_POSITION, END_POSITION);
    verify(batchReader).read(NEXT_START_POSITION, END_POSITION);
    verifyNoMoreInteractions(batchReader);
  }

  @Test
  public void readerShouldMergeMultipleBatchResultsIncludingEmptyShards() throws Exception {
    List<ShuffleEntry> e1s = new ArrayList<>();
    List<ShuffleEntry> e2s = new ArrayList<>();
    ShuffleEntry e3 = newShuffleEntry(KEY, SKEY, VALUE);
    List<ShuffleEntry> e3s = Collections.singletonList(e3);
    when(batchReader.read(START_POSITION, END_POSITION))
        .thenReturn(new ShuffleBatchReader.Batch(e1s, NEXT_START_POSITION));
    when(batchReader.read(NEXT_START_POSITION, END_POSITION))
        .thenReturn(new ShuffleBatchReader.Batch(e2s, SECOND_NEXT_START_POSITION));
    when(batchReader.read(SECOND_NEXT_START_POSITION, END_POSITION))
        .thenReturn(new ShuffleBatchReader.Batch(e3s, null));
    List<ShuffleEntry> results = newArrayList(reader.read(START_POSITION, END_POSITION));
    assertThat(results, contains(e3));

    verify(batchReader).read(START_POSITION, END_POSITION);
    verify(batchReader).read(NEXT_START_POSITION, END_POSITION);
    verify(batchReader).read(SECOND_NEXT_START_POSITION, END_POSITION);
    verifyNoMoreInteractions(batchReader);
  }
}
