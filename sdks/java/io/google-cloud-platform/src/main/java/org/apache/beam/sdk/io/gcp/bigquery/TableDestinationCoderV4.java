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
import java.util.Map;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Coder} for {@link TableDestination} that includes time partitioning, clustering and
 * table label information. Users must opt in to this version of the coder by setting {@link
 * BigQueryIO.Write#withTableLabels()}, otherwise {@link TableDestinationCoderV2} or {@link
 * TableDestinationCoderV3} will be used and labels will be discarded.
 */
public class TableDestinationCoderV4 extends AtomicCoder<TableDestination> {
  private static final TableDestinationCoderV4 INSTANCE = new TableDestinationCoderV4();
  private static final Coder<@Nullable Map<String, String>> labelsCoder =
      NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

  public static TableDestinationCoderV4 of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableDestination value, OutputStream outStream) throws IOException {
    TableDestinationCoderV3.of().encode(value, outStream);
    labelsCoder.encode(value.getLabels(), outStream);
  }

  @Override
  public TableDestination decode(InputStream inStream) throws IOException {
    TableDestination destination = TableDestinationCoderV3.of().decode(inStream);
    Map<String, String> labels = labelsCoder.decode(inStream);
    return destination.withLabels(labels);
  }

  // MapCoder on its own is not deterministic because it writes entries in the map's iteration
  // order. TableDestination stores labels in a sorted map, so equal label sets always encode to
  // the same bytes
  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
