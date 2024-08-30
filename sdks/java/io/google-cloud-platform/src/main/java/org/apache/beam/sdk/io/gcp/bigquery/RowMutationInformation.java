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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This class indicates how to apply a row update to BigQuery. A sequence number must always be
 * supplied to order the updates. Incorrect sequence numbers will result in unexpected state in the
 * BigQuery table.
 */
@AutoValue
public abstract class RowMutationInformation {
  public enum MutationType {
    // Upsert the row based on the primary key. Either inserts or replaces an existing row. This is
    // an idempotent
    // operation - multiple updates will simply overwrite the same row.
    UPSERT,
    // Delete the row with the matching primary key.
    DELETE
  }

  public abstract MutationType getMutationType();

  /**
   * The sequence number used to drive the order of applied row mutations. @deprecated {@link
   * #getChangeSequenceNumber()} replaces this field as the BigQuery API instead supports the use of
   * a string.
   */
  @Deprecated
  @Nullable
  public abstract Long getSequenceNumber();

  /**
   * The value supplied to the BigQuery {@code _CHANGE_SEQUENCE_NUMBER} pseudo-column. See {@link
   * #of(MutationType, String)} for more details.
   */
  public abstract String getChangeSequenceNumber();

  /**
   * Instantiate {@link RowMutationInformation} with {@link MutationType} and the {@param
   * sequenceNumber}. @deprecated - instantiates {@link RowMutationInformation} via {@link
   * #of(MutationType, String)} forwarding the {@param sequenceNumber} value using {@link
   * Long#toHexString(long)}. {@param sequenceNumber} values {@code < 0} will throw an error.
   */
  @Deprecated
  public static RowMutationInformation of(MutationType mutationType, long sequenceNumber) {
    checkArgument(sequenceNumber >= 0, "sequenceNumber must be non-negative");
    return new AutoValue_RowMutationInformation(
        mutationType, null, Long.toHexString(sequenceNumber));
  }

  /**
   * Instantiate {@link RowMutationInformation} with {@link MutationType} and the {@param
   * changeSequenceNumber}, which sets the BigQuery API {@code _CHANGE_SEQUENCE_NUMBER} pseudo
   * column, enabling custom user-supplied ordering of {@link RowMutation}s.
   *
   * <p>Requirements for the {@param changeSequenceNumber}:
   *
   * <ul>
   *   <li>fixed format {@code String} in hexadecimal format
   *   <li><strong>do not use hexadecimals encoded from negative numbers</strong>
   *   <li>each hexadecimal string separated into sections by forward slash: {@code /}
   *   <li>up to four sections allowed
   *   <li>each section is limited to {@code 16} hexadecimal characters: {@code 0-9}, {@code A-F},
   *       or {@code a-f}
   *   <li>The allowable range supported are values between {@code 0/0/0/0} and {@code
   *       FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF/FFFFFFFFFFFFFFFF}
   * </ul>
   *
   * <p>Below are some {@param changeSequenceNumber} scenarios:
   *
   * <table>
   *     <tr>
   *         <th>Record #1: {@param changeSequenceNumber}</th>
   *         <th>Record #2: {@param changeSequenceNumber}</th>
   *         <th>BigQuery API compares as</th>
   *     </tr>
   *     <tr>
   *         <td>{@code "B"}</td>
   *         <td>{@code "ABC"}</td>
   *         <td>Record #2 is considered the latest record: {@code 'ABC' > 'B' (i.e. '2748' > '11')}</td>
   *     </tr>
   *     <tr>
   *         <td>{@code "FFF/B"}</td>
   *         <td>{@code "FFF/ABC"}</td>
   *         <td>Record #2 is considered the latest record: {@code "FFF/B" > "FFF/ABC" (i.e. "4095/2748" > "4095/11")}</td>
   *     </tr>
   *     <tr>
   *         <td>{@code "BA/FFFFFFFF"}</td>
   *         <td>{@code "ABC"}</td>
   *         <td>Record #2 is considered the latest record: {@code "ABC" > "BA/FFFFFFFF" (i.e. "2748" > "186/4294967295")}</td>
   *     </tr>
   *     <tr>
   *         <td>{@code "FFF/ABC"}</td>
   *         <td>{@code "ABC"}</td>
   *         <td>Record #1 is considered the latest record: {@code "FFF/ABC" > "ABC" (i.e. "4095/2748" > "2748")}</td>
   *     </tr>
   *     <tr>
   *         <td>{@code "FFF"}</td>
   *         <td>{@code "FFF"}</td>
   *         <td>Record #1 and #2 change sequence number identical; BigQuery uses system ingestion time to take precedence over previously ingested records.</td>
   *     </tr>
   * </table>
   *
   * <p>Below are some code examples.
   *
   * <ul>
   *   <li>{@code RowMutationInformation.of(UPSERT, "FFF/ABC")}
   *   <li>Using Apache Commons {@link org.apache.commons.codec.binary.Hex#encodeHexString} (Java
   *       17+ users can use {@code HexFormat}){@code RowMutationInformation.of(UPSERT,
   *       Hex.encodeHexString("2024-04-30 11:19:44 UTC".getBytes(StandardCharsets.UTF_8)))}
   *   <li>Using {@link Long#toHexString}: {@code RowMutationInformation.of(DELETE,
   *       Long.toHexString(123L))}
   * </ul>
   *
   * See <a
   * href="https://cloud.google.com/bigquery/docs/change-data-capture#manage_custom_ordering">
   * https://cloud.google.com/bigquery/docs/change-data-capture#manage_custom_ordering </a> for more
   * details.
   */
  public static RowMutationInformation of(MutationType mutationType, String changeSequenceNumber) {
    checkArgument(
        !Strings.isNullOrEmpty(changeSequenceNumber), "changeSequenceNumber must not be empty");
    checkArgument(
        StorageApiCDC.EXPECTED_SQN_PATTERN.asPredicate().test(changeSequenceNumber),
        String.format(
            "changeSequenceNumber: %s does not match expected pattern: %s",
            changeSequenceNumber, StorageApiCDC.EXPECTED_SQN_PATTERN.pattern()));
    return new AutoValue_RowMutationInformation(mutationType, null, changeSequenceNumber);
  }
}
