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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.reconciler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

public class OrphanedMetadataCleanerTest {

  @Test
  public void testOrphanedNewPartitions() {
    OrphanedMetadataCleaner orphanedMetadataCleaner = new OrphanedMetadataCleaner();
    // Not waiting long enough
    NewPartition newPartition1 =
        new NewPartition(
            ByteStringRange.create("A", "B"),
            Collections.emptyList(),
            Instant.EPOCH,
            Instant.now().minus(Duration.standardMinutes(10)));
    orphanedMetadataCleaner.addIncompleteNewPartitions(newPartition1);
    // Orphaned long enough. Should be cleaned up
    NewPartition newPartition2 =
        new NewPartition(
            ByteStringRange.create("B", "C"),
            Collections.emptyList(),
            Instant.EPOCH,
            Instant.now().minus(Duration.standardMinutes(15)).minus(Duration.standardSeconds(1)));
    orphanedMetadataCleaner.addIncompleteNewPartitions(newPartition2);
    // Just on the edge of not orphaned long enough
    NewPartition newPartition3 =
        new NewPartition(
            ByteStringRange.create("C", "D"),
            Collections.emptyList(),
            Instant.EPOCH,
            Instant.now().minus(Duration.standardMinutes(15)).plus(Duration.standardSeconds(1)));
    orphanedMetadataCleaner.addIncompleteNewPartitions(newPartition3);
    // Orphaned long enough, but it overlaps with a missing partition, so not cleaned up.
    NewPartition newPartition4 =
        new NewPartition(
            ByteStringRange.create("D", "F"),
            Collections.emptyList(),
            Instant.EPOCH,
            Instant.now().minus(Duration.standardMinutes(15)).minus(Duration.standardSeconds(1)));
    orphanedMetadataCleaner.addIncompleteNewPartitions(newPartition4);
    List<ByteStringRange> missingPartitions = new ArrayList<>();

    // Haven't called addMissingPartitions, so we don't know if there are any missing partitions.
    assertTrue(orphanedMetadataCleaner.getOrphanedNewPartitions().isEmpty());

    missingPartitions.add(ByteStringRange.create("E", "G"));
    missingPartitions.add(ByteStringRange.create("H", "I"));
    orphanedMetadataCleaner.addMissingPartitions(missingPartitions);

    assertEquals(
        Collections.singletonList(newPartition2),
        orphanedMetadataCleaner.getOrphanedNewPartitions());
  }
}
