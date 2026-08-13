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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Schema-pin and {@link RewriteResult.Merge} combiner tests for {@link RewriteResult}. */
@RunWith(JUnit4.class)
public class RewriteResultTest {

  @Test
  public void mergeSumsNumericFieldsAndTakesFirstNonNullIds() {
    // A planning fragment (ids + planned counts) + a commit fragment (commit counts): the merged
    // row sums the numerics and carries the planning fragment's ids.
    RewriteResult plan =
        RewriteResult.builder()
            .setOperationId("op-1")
            .setStartingSnapshotId(42L)
            .setPlannedParentGroups(3L)
            .setPlannedFiles(9L)
            .setPlannedBytes(900L)
            .build();
    RewriteResult commit =
        RewriteResult.builder()
            .setCommittedSnapshots(2L)
            .setFilesAdded(2L)
            .setFilesRemoved(9L)
            .setRewrittenBytes(900L)
            .build();

    RewriteResult merged = combine(new RewriteResult.Merge(), plan, commit);

    assertEquals("op-1", merged.getOperationId());
    assertEquals(Long.valueOf(42L), merged.getStartingSnapshotId());
    assertEquals(3L, merged.getPlannedParentGroups());
    assertEquals(9L, merged.getPlannedFiles());
    assertEquals(900L, merged.getPlannedBytes());
    assertEquals(2L, merged.getCommittedSnapshots());
    assertEquals(2L, merged.getFilesAdded());
    assertEquals(9L, merged.getFilesRemoved());
    assertEquals(900L, merged.getRewrittenBytes());
  }

  @Test
  public void identityDoesNotChangeAResult() {
    RewriteResult.Merge fn = new RewriteResult.Merge();
    RewriteResult x =
        RewriteResult.builder()
            .setOperationId("op-x")
            .setStartingSnapshotId(7L)
            .setPlannedParentGroups(5L)
            .setFailedRewriteParents(2L)
            .setCommittedSnapshots(3L)
            .build();
    assertEquals(x, combine(fn, x, RewriteResult.zeros()));
    assertEquals(x, combine(fn, RewriteResult.zeros(), x));
  }

  @Test
  public void mergeIsAssociativeAndCommutativeOverAShuffledList() {
    List<RewriteResult> frags = new ArrayList<>();
    frags.add(
        RewriteResult.builder()
            .setOperationId("op-9")
            .setStartingSnapshotId(100L)
            .setPlannedParentGroups(6L)
            .setPlannedFiles(18L)
            .setPlannedBytes(1800L)
            .build());
    frags.add(RewriteResult.builder().setFailedRewriteParents(2L).build());
    frags.add(
        RewriteResult.builder()
            .setCommittedSnapshots(1L)
            .setFilesAdded(1L)
            .setFilesRemoved(4L)
            .setRewrittenBytes(400L)
            .build());
    frags.add(RewriteResult.builder().setFailedCommits(1L).build());

    RewriteResult.Merge fn = new RewriteResult.Merge();
    RewriteResult expected = foldLeft(fn, frags);

    for (long seed : new long[] {1L, 2L, 3L, 12345L}) {
      List<RewriteResult> shuffled = new ArrayList<>(frags);
      Collections.shuffle(shuffled, new Random(seed));
      assertEquals("fold order must not change the sum", expected, foldLeft(fn, shuffled));
    }

    // Two-accumulator grouping then mergeAccumulators must equal the single fold.
    RewriteResult accA = foldLeft(fn, frags.subList(0, 2));
    RewriteResult accB = foldLeft(fn, frags.subList(2, 4));
    RewriteResult regrouped =
        fn.extractOutput(fn.mergeAccumulators(java.util.Arrays.asList(accA, accB)));
    assertEquals(expected, regrouped);

    // Sanity on the aggregate content.
    assertEquals("op-9", expected.getOperationId());
    assertEquals(6L, expected.getPlannedParentGroups());
    assertEquals(2L, expected.getFailedRewriteParents());
    assertEquals(1L, expected.getCommittedSnapshots());
    assertEquals(1L, expected.getFailedCommits());
  }

  private static RewriteResult combine(RewriteResult.Merge fn, RewriteResult... inputs) {
    return foldLeft(fn, java.util.Arrays.asList(inputs));
  }

  private static RewriteResult foldLeft(RewriteResult.Merge fn, List<RewriteResult> inputs) {
    RewriteResult acc = fn.createAccumulator();
    for (RewriteResult in : inputs) {
      acc = fn.addInput(acc, in);
    }
    return fn.extractOutput(acc);
  }
}
