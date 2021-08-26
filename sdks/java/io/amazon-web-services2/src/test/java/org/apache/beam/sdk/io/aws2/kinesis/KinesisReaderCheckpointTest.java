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
package org.apache.beam.sdk.io.aws2.kinesis;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/** * */
@RunWith(MockitoJUnitRunner.class)
public class KinesisReaderCheckpointTest {

  @Mock private ShardCheckpoint a, b, c;

  private KinesisReaderCheckpoint checkpoint;

  @Before
  public void setUp() {
    checkpoint = new KinesisReaderCheckpoint(asList(a, b, c));
  }

  @Test
  public void splitsCheckpointAccordingly() {
    verifySplitInto(1);
    verifySplitInto(2);
    verifySplitInto(3);
    verifySplitInto(4);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void isImmutable() {
    Iterator<ShardCheckpoint> iterator = checkpoint.iterator();
    iterator.remove();
  }

  private void verifySplitInto(int size) {
    List<KinesisReaderCheckpoint> split = checkpoint.splitInto(size);
    assertThat(Iterables.concat(split)).containsOnly(a, b, c);
    assertThat(split).hasSize(Math.min(size, 3));
  }
}
