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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecordMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.junit.Before;
import org.junit.Test;

public class PostProcessingMetricsDoFnTest {

  private ChangeStreamMetrics changeStreamMetrics;
  private OutputReceiver<DataChangeRecord> receiver;
  private PostProcessingMetricsDoFn processingMetricsDoFn;

  @Before
  public void setUp() {
    receiver = mock(OutputReceiver.class);
    changeStreamMetrics = mock(ChangeStreamMetrics.class);
    processingMetricsDoFn = new PostProcessingMetricsDoFn(changeStreamMetrics);
  }

  @Test
  public void testPostProcessingMetrics() {
    DataChangeRecord dataChangeRecord =
        new DataChangeRecord(
            "partitionToken",
            Timestamp.ofTimeMicroseconds(1L),
            "serverTransactionId",
            true,
            "recordSequence",
            "tableName",
            Arrays.asList(
                new ColumnType("column1", new TypeCode("type1"), true, 1L),
                new ColumnType("column2", new TypeCode("type2"), false, 2L)),
            Collections.singletonList(
                new Mod(
                    "{\"column1\": \"value1\"}",
                    "{\"column2\": \"oldValue2\"}",
                    "{\"column2\": \"newValue2\"}")),
            ModType.UPDATE,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            10L,
            2L,
            "transactionTag",
            true,
            ChangeStreamRecordMetadata.newBuilder()
                .withRecordStreamStartedAt(Timestamp.ofTimeMicroseconds(1L))
                .withRecordStreamEndedAt(Timestamp.ofTimeMicroseconds(2L))
                .build());
    doNothing().when(changeStreamMetrics).incDataRecordCounter();
    doNothing().when(changeStreamMetrics).updateDataRecordCommittedToEmitted(any());
    processingMetricsDoFn.processElement(dataChangeRecord, receiver);
    verify(changeStreamMetrics, times(1)).incDataRecordCounter();
    verify(changeStreamMetrics, times(1)).updateDataRecordCommittedToEmitted(any());
  }
}
