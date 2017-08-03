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

import com.google.api.services.bigquery.model.TimePartitioning;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** A coder for {@link TableDestination} objects. */
public class TableDestinationCoder extends AtomicCoder<TableDestination> {
  private static final TableDestinationCoder INSTANCE = new TableDestinationCoder();
  private static final Coder<String> tableSpecCoder = StringUtf8Coder.of();
  private static final Coder<String> tableDescriptionCoder = NullableCoder.of(StringUtf8Coder.of());
  private static final Coder<String> timePartitioningCoder = NullableCoder.of(StringUtf8Coder.of());

  public static TableDestinationCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableDestination value, OutputStream outStream)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null value");
    }
    tableSpecCoder.encode(value.getTableSpec(), outStream);
    tableDescriptionCoder.encode(value.getTableDescription(), outStream);
    timePartitioningCoder.encode(value.getJsonTimePartitioning(), outStream);
  }

  @Override
  public TableDestination decode(InputStream inStream) throws IOException {
    String tableSpec = tableSpecCoder.decode(inStream);
    String tableDescription = tableDescriptionCoder.decode(inStream);
    String jsonTimePartitioning = null;
    try {
      jsonTimePartitioning = timePartitioningCoder.decode(inStream);
    } catch (IOException e) {
      // This implies we're decoding old state that did not contain TimePartitioning. Continue.
    }
    return new TableDestination(tableSpec, tableDescription, jsonTimePartitioning);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
