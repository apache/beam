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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExpireSnapshotsResultTest {

  @Test
  public void testZerosIdentity() {
    ExpireSnapshotsResult zeros = ExpireSnapshotsResult.zeros();
    assertEquals(0L, zeros.getDeletedDataFilesCount());
    assertEquals(0L, zeros.getDeletedPositionDeleteFilesCount());
    assertEquals(0L, zeros.getDeletedEqualityDeleteFilesCount());
    assertEquals(0L, zeros.getDeletedManifestsCount());
    assertEquals(0L, zeros.getDeletedManifestListsCount());
    assertEquals(0L, zeros.getDeletedStatisticsFilesCount());
    assertEquals(0L, zeros.getExpiredSnapshotsCount());
  }

  @Test
  public void testMergeFragments() {
    ExpireSnapshotsResult a =
        ExpireSnapshotsResult.builder()
            .setDeletedDataFilesCount(5L)
            .setDeletedPositionDeleteFilesCount(2L)
            .setDeletedEqualityDeleteFilesCount(1L)
            .setDeletedManifestsCount(3L)
            .setDeletedManifestListsCount(1L)
            .setDeletedStatisticsFilesCount(0L)
            .setExpiredSnapshotsCount(1L)
            .build();

    ExpireSnapshotsResult b =
        ExpireSnapshotsResult.builder()
            .setDeletedDataFilesCount(10L)
            .setDeletedPositionDeleteFilesCount(0L)
            .setDeletedEqualityDeleteFilesCount(3L)
            .setDeletedManifestsCount(2L)
            .setDeletedManifestListsCount(1L)
            .setDeletedStatisticsFilesCount(1L)
            .setExpiredSnapshotsCount(1L)
            .build();

    ExpireSnapshotsResult merged = ExpireSnapshotsResult.merge(a, b);
    assertEquals(15L, merged.getDeletedDataFilesCount());
    assertEquals(2L, merged.getDeletedPositionDeleteFilesCount());
    assertEquals(4L, merged.getDeletedEqualityDeleteFilesCount());
    assertEquals(5L, merged.getDeletedManifestsCount());
    assertEquals(2L, merged.getDeletedManifestListsCount());
    assertEquals(1L, merged.getDeletedStatisticsFilesCount());
    assertEquals(2L, merged.getExpiredSnapshotsCount());
  }

  @Test
  public void testCombineFn() {
    ExpireSnapshotsResult.Merge mergeFn = new ExpireSnapshotsResult.Merge();
    ExpireSnapshotsResult acc = mergeFn.createAccumulator();
    assertEquals(ExpireSnapshotsResult.zeros(), acc);

    ExpireSnapshotsResult item1 =
        ExpireSnapshotsResult.builder()
            .setDeletedDataFilesCount(3L)
            .setExpiredSnapshotsCount(1L)
            .build();
    ExpireSnapshotsResult item2 =
        ExpireSnapshotsResult.builder()
            .setDeletedDataFilesCount(7L)
            .setDeletedManifestsCount(2L)
            .build();

    acc = mergeFn.addInput(acc, item1);
    acc = mergeFn.addInput(acc, item2);

    ExpireSnapshotsResult output = mergeFn.extractOutput(acc);
    assertEquals(10L, output.getDeletedDataFilesCount());
    assertEquals(2L, output.getDeletedManifestsCount());
    assertEquals(1L, output.getExpiredSnapshotsCount());

    ExpireSnapshotsResult mergedAcc =
        mergeFn.mergeAccumulators(Arrays.asList(item1, item2, ExpireSnapshotsResult.zeros()));
    assertEquals(10L, mergedAcc.getDeletedDataFilesCount());
    assertEquals(2L, mergedAcc.getDeletedManifestsCount());
    assertEquals(1L, mergedAcc.getExpiredSnapshotsCount());

    ExpireSnapshotsResult emptyMerge = mergeFn.mergeAccumulators(Collections.emptyList());
    assertEquals(ExpireSnapshotsResult.zeros(), emptyMerge);
  }

  @Test
  public void testSchemaCoderSerialization() throws NoSuchSchemaException, IOException {
    Coder<ExpireSnapshotsResult> coder =
        SchemaRegistry.createDefault().getSchemaCoder(ExpireSnapshotsResult.class);

    ExpireSnapshotsResult original =
        ExpireSnapshotsResult.builder()
            .setDeletedDataFilesCount(123L)
            .setDeletedPositionDeleteFilesCount(45L)
            .setDeletedEqualityDeleteFilesCount(6L)
            .setDeletedManifestsCount(78L)
            .setDeletedManifestListsCount(9L)
            .setDeletedStatisticsFilesCount(10L)
            .setExpiredSnapshotsCount(11L)
            .build();

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    coder.encode(original, out);
    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    ExpireSnapshotsResult decoded = coder.decode(in);

    assertEquals(original, decoded);
  }
}
