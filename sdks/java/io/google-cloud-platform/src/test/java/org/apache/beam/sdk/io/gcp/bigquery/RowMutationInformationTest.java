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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RowMutationInformationTest {
  @Test
  public void givenLong_SQN_EQ_Zero_encodesAndInstantiates() {
    long sqn = 0L;
    RowMutationInformation got =
        RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, sqn);
    assertNotNull(got.getChangeSequenceNumber());
    assertEquals("0", got.getChangeSequenceNumber());
  }

  @Test
  public void givenLong_SQN_GT_Zero_encodesAndInstantiates() {
    long sqn = 6L;
    RowMutationInformation got =
        RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, sqn);
    assertNotNull(got.getChangeSequenceNumber());
    assertEquals("6", got.getChangeSequenceNumber());
  }

  @Test
  public void givenLong_SQN_EQ_Max_encodesAndInstantiates() {
    long sqn = Long.MAX_VALUE;
    RowMutationInformation got =
        RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, sqn);
    assertNotNull(got.getChangeSequenceNumber());
    assertEquals("7fffffffffffffff", got.getChangeSequenceNumber());
  }

  @Test
  public void givenLong_SQL_LT_Zero_throws() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, -1L));
    assertEquals("sequenceNumber must be non-negative", error.getMessage());
  }

  @Test
  public void givenSimpleHex_encodesAndInstantiates() {
    String sqn = "FFF/ABC/012/AAA";
    RowMutationInformation got =
        RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, sqn);
    assertNotNull(got.getChangeSequenceNumber());
    assertEquals(sqn, got.getChangeSequenceNumber());
  }

  @Test
  public void givenTooManySegments_throws() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, "0/0/0/0/0"));
    assertEquals(
        "changeSequenceNumber: 0/0/0/0/0 does not match expected pattern: ^([0-9A-Fa-f]{1,16})(/([0-9A-Fa-f]{1,16})){0,3}$",
        error.getMessage());
  }

  @Test
  public void givenEmptyString_throws() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, ""));
    assertEquals("changeSequenceNumber must not be empty", error.getMessage());
  }

  @Test
  public void givenEmptySegment_throws() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> RowMutationInformation.of(RowMutationInformation.MutationType.UPSERT, "0/1//3"));
    assertEquals(
        "changeSequenceNumber: 0/1//3 does not match expected pattern: ^([0-9A-Fa-f]{1,16})(/([0-9A-Fa-f]{1,16})){0,3}$",
        error.getMessage());
  }

  @Test
  public void givenSingleSegmentTooLarge_throws() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.UPSERT, "12345678901234567"));
    assertEquals(
        "changeSequenceNumber: 12345678901234567 does not match expected pattern: ^([0-9A-Fa-f]{1,16})(/([0-9A-Fa-f]{1,16})){0,3}$",
        error.getMessage());
  }

  @Test
  public void givenAddlSegmentTooLarge_throws() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RowMutationInformation.of(
                    RowMutationInformation.MutationType.UPSERT, "0/12345678901234567"));
    assertEquals(
        "changeSequenceNumber: 0/12345678901234567 does not match expected pattern: ^([0-9A-Fa-f]{1,16})(/([0-9A-Fa-f]{1,16})){0,3}$",
        error.getMessage());
  }
}
