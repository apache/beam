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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RewriteConfigurationTest {
  @Test
  public void defaults() {
    RewriteDataFiles.Configuration c = RewriteDataFiles.Configuration.builder().build();
    assertFalse(c.partialProgressEnabled());
    assertEquals(10, c.maxCommits());
    assertEquals(10, c.maxFailedCommits()); // defaults to maxCommits
    assertTrue(c.useStartingSequenceNumber());
    assertEquals(Long.MAX_VALUE, c.maxRewriteBytes());
    assertFalse(c.caseSensitive());
  }

  @Test
  public void maxFailedCommitsFollowsMaxCommits() {
    RewriteDataFiles.Configuration c =
        RewriteDataFiles.Configuration.builder().setMaxCommits(3).build();
    assertEquals(3, c.maxFailedCommits());
  }

  @Test
  public void explicitMaxFailedCommitsWins() {
    RewriteDataFiles.Configuration c =
        RewriteDataFiles.Configuration.builder().setMaxCommits(5).setMaxFailedCommits(2).build();
    assertEquals(2, c.maxFailedCommits());
  }

  @Test
  public void invalidMaxCommitsWhenPartial() {
    RewriteDataFiles.Configuration c =
        RewriteDataFiles.Configuration.builder()
            .setPartialProgressEnabled(true)
            .setMaxCommits(0)
            .build();
    assertThrows(IllegalArgumentException.class, c::validate);
  }

  @Test
  public void invalidMaxRewriteBytes() {
    RewriteDataFiles.Configuration c =
        RewriteDataFiles.Configuration.builder().setMaxRewriteBytes(0L).build();
    assertThrows(IllegalArgumentException.class, c::validate);
  }

  @Test
  public void validDefaultsPassValidation() {
    RewriteDataFiles.Configuration.builder().build().validate(); // no throw
  }

  @Test
  public void reservedSnapshotPropertyKeyRejected() {
    // D1: user snapshot-summary keys must not collide with the beam.rewrite.* idempotency stamps.
    RewriteDataFiles.Configuration c =
        RewriteDataFiles.Configuration.builder()
            .setSnapshotProperties(ImmutableMap.of("beam.rewrite.operation-id", "hijack"))
            .build();
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, c::validate);
    assertTrue(ex.getMessage().contains("reserved"));
  }

  @Test
  public void branchAndSnapshotIdTogetherRejected() {
    // R13-D: branch and snapshotId are mutually exclusive — pinning an explicit snapshot on a
    // branch run would validate the commit against main's ancestry, not the branch's.
    RewriteDataFiles.Configuration c =
        RewriteDataFiles.Configuration.builder().setBranch("audit").setSnapshotId(123L).build();
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, c::validate);
    assertTrue(ex.getMessage().contains("only one of branch or snapshotId"));
  }

  @Test
  public void userSnapshotPropertyKeyAccepted() {
    RewriteDataFiles.Configuration c =
        RewriteDataFiles.Configuration.builder()
            .setSnapshotProperties(ImmutableMap.of("team", "data-platform"))
            .build();
    c.validate(); // no throw
    assertEquals("data-platform", c.snapshotProperties().get("team"));
  }
}
