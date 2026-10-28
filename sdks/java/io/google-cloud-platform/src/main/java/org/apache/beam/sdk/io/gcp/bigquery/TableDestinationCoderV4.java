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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Coder} for {@link TableDestination} that encodes time partitioning, clustering and range
 * partitioning. {@link BigQueryIO.Write} switches to it only when one of its range partitioning
 * options is set. Otherwise it uses {@link TableDestinationCoderV3} or {@link
 * TableDestinationCoderV2}, which drop range partitioning.
 */
public class TableDestinationCoderV4 extends AtomicCoder<TableDestination> {
  private static final TableDestinationCoderV4 INSTANCE = new TableDestinationCoderV4();
  private static final Coder<@Nullable String> rangePartitioningCoder =
      NullableCoder.of(StringUtf8Coder.of());

  public static TableDestinationCoderV4 of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableDestination value, OutputStream outStream) throws IOException {
    TableDestinationCoderV3.of().encode(value, outStream);
    rangePartitioningCoder.encode(value.getJsonRangePartitioning(), outStream);
  }

  @Override
  public TableDestination decode(InputStream inStream) throws IOException {
    TableDestination destination = TableDestinationCoderV3.of().decode(inStream);
    String jsonRangePartitioning = rangePartitioningCoder.decode(inStream);
    return new TableDestination(
        destination.getTableSpec(),
        destination.getTableDescription(),
        destination.getJsonTimePartitioning(),
        destination.getJsonClustering(),
        jsonRangePartitioning);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
