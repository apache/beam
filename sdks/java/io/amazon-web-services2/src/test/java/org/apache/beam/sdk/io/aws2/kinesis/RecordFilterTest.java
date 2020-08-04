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

import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

/** * */
@RunWith(MockitoJUnitRunner.class)
public class RecordFilterTest {

  @Mock private ShardCheckpoint checkpoint;
  @Mock private KinesisRecord record1, record2, record3, record4, record5;

  @Test
  public void shouldFilterOutRecordsBeforeOrAtCheckpoint() {
    when(checkpoint.isBeforeOrAt(record1)).thenReturn(false);
    when(checkpoint.isBeforeOrAt(record2)).thenReturn(true);
    when(checkpoint.isBeforeOrAt(record3)).thenReturn(true);
    when(checkpoint.isBeforeOrAt(record4)).thenReturn(false);
    when(checkpoint.isBeforeOrAt(record5)).thenReturn(true);
    List<KinesisRecord> records = Lists.newArrayList(record1, record2, record3, record4, record5);
    RecordFilter underTest = new RecordFilter();

    List<KinesisRecord> retainedRecords = underTest.apply(records, checkpoint);

    Assertions.assertThat(retainedRecords).containsOnly(record2, record3, record5);
  }

  @Test
  public void shouldNotFailOnEmptyList() {
    List<KinesisRecord> records = Collections.emptyList();
    RecordFilter underTest = new RecordFilter();

    List<KinesisRecord> retainedRecords = underTest.apply(records, checkpoint);

    Assertions.assertThat(retainedRecords).isEmpty();
  }
}
