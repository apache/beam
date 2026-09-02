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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import java.util.Objects;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueKind;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * One change record carried through the CDC sink's shuffle.
 *
 * <p>{@link ValueKind} is reified because it's not preserved across a {@code GroupByKey}.
 */
final class CdcRecord {

  private final Row data;
  private final ValueKind kind;
  private final long sequenceNumber;

  private CdcRecord(Row data, ValueKind kind, long sequenceNumber) {
    this.data = data;
    this.kind = kind;
    this.sequenceNumber = sequenceNumber;
  }

  public static CdcRecord of(Row data, ValueKind kind, long sequenceNumber) {
    return new CdcRecord(data, kind, sequenceNumber);
  }

  public Row getData() {
    return data;
  }

  public ValueKind getKind() {
    return kind;
  }

  public long getSequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CdcRecord)) {
      return false;
    }
    CdcRecord that = (CdcRecord) o;
    return sequenceNumber == that.sequenceNumber && kind == that.kind && data.equals(that.data);
  }

  @Override
  public int hashCode() {
    return Objects.hash(data, kind, sequenceNumber);
  }

  @Override
  public String toString() {
    return "CdcRecord{"
        + "data="
        + data
        + ", kind="
        + kind
        + ", sequenceNumber="
        + sequenceNumber
        + '}';
  }
}
