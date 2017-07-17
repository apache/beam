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
package org.apache.beam.sdk.io.kinesis;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Tests {@link KinesisReader}.
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisReaderTest {

  @Mock
  private SimplifiedKinesisClient kinesis;
  @Mock
  private CheckpointGenerator generator;
  @Mock
  private ShardCheckpoint firstCheckpoint, secondCheckpoint;
  @Mock
  private ShardRecordsIterator firstIterator, secondIterator;
  @Mock
  private KinesisRecord a, b, c, d;

  private KinesisReader reader;

  @Before
  public void setUp() throws IOException, TransientKinesisException {
    when(generator.generate(kinesis)).thenReturn(new KinesisReaderCheckpoint(
        asList(firstCheckpoint, secondCheckpoint)
    ));
    when(firstCheckpoint.getShardRecordsIterator(kinesis)).thenReturn(firstIterator);
    when(secondCheckpoint.getShardRecordsIterator(kinesis)).thenReturn(secondIterator);
    when(firstIterator.next()).thenReturn(CustomOptional.<KinesisRecord>absent());
    when(secondIterator.next()).thenReturn(CustomOptional.<KinesisRecord>absent());

    reader = new KinesisReader(kinesis, generator, null);
  }

  @Test
  public void startReturnsFalseIfNoDataAtTheBeginning() throws IOException {
    assertThat(reader.start()).isFalse();
  }

  @Test(expected = NoSuchElementException.class)
  public void throwsNoSuchElementExceptionIfNoData() throws IOException {
    reader.start();
    reader.getCurrent();
  }

  @Test
  public void startReturnsTrueIfSomeDataAvailable() throws IOException,
      TransientKinesisException {
    when(firstIterator.next()).
        thenReturn(CustomOptional.of(a)).
        thenReturn(CustomOptional.<KinesisRecord>absent());

    assertThat(reader.start()).isTrue();
  }

  @Test
  public void advanceReturnsFalseIfThereIsTransientExceptionInKinesis()
      throws IOException, TransientKinesisException {
    reader.start();

    when(firstIterator.next()).thenThrow(TransientKinesisException.class);

    assertThat(reader.advance()).isFalse();
  }

  @Test
  public void readsThroughAllDataAvailable() throws IOException, TransientKinesisException {
    when(firstIterator.next()).
        thenReturn(CustomOptional.<KinesisRecord>absent()).
        thenReturn(CustomOptional.of(a)).
        thenReturn(CustomOptional.<KinesisRecord>absent()).
        thenReturn(CustomOptional.of(b)).
        thenReturn(CustomOptional.<KinesisRecord>absent());

    when(secondIterator.next()).
        thenReturn(CustomOptional.of(c)).
        thenReturn(CustomOptional.<KinesisRecord>absent()).
        thenReturn(CustomOptional.of(d)).
        thenReturn(CustomOptional.<KinesisRecord>absent());

    assertThat(reader.start()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(c);
    assertThat(reader.advance()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(a);
    assertThat(reader.advance()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(d);
    assertThat(reader.advance()).isTrue();
    assertThat(reader.getCurrent()).isEqualTo(b);
    assertThat(reader.advance()).isFalse();
  }

}
