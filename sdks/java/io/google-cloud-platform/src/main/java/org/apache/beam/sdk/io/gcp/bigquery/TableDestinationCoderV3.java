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

/**
 * A {@link Coder} for {@link TableDestination} that includes time partitioning and clustering
 * information. Users must opt in to this version of the coder by setting one of the clustering
 * options on {@link BigQueryIO.Write}, otherwise {@link TableDestinationCoderV2} will be used and
 * clustering information will be discarded.
 */
public class TableDestinationCoderV3 extends AtomicCoder<TableDestination> {
  private static final TableDestinationCoderV3 INSTANCE = new TableDestinationCoderV3();
  private static final Coder<String> timePartitioningCoder = NullableCoder.of(StringUtf8Coder.of());
  private static final Coder<String> clusteringCoder = NullableCoder.of(StringUtf8Coder.of());

  public static TableDestinationCoderV3 of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableDestination value, OutputStream outStream) throws IOException {
    TableDestinationCoder.of().encode(value, outStream);
    timePartitioningCoder.encode(value.getJsonTimePartitioning(), outStream);
    clusteringCoder.encode(value.getJsonClustering(), outStream);
  }

  @Override
  public TableDestination decode(InputStream inStream) throws IOException {
    TableDestination destination = TableDestinationCoder.of().decode(inStream);
    String jsonTimePartitioning = timePartitioningCoder.decode(inStream);
    String jsonClustering = clusteringCoder.decode(inStream);
    return new TableDestination(
        destination.getTableSpec(),
        destination.getTableDescription(),
        jsonTimePartitioning,
        jsonClustering);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
