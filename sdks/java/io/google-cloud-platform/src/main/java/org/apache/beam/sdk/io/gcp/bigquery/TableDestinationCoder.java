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
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** A coder for {@link TableDestination} objects. */
public class TableDestinationCoder extends AtomicCoder<TableDestination> {
  private static final TableDestinationCoder INSTANCE = new TableDestinationCoder();
  private static final Coder<String> tableSpecCoder = StringUtf8Coder.of();
  private static final Coder<String> tableDescriptionCoder = NullableCoder.of(StringUtf8Coder.of());

  private TableDestinationCoder() {}

  public static TableDestinationCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableDestination value, OutputStream outStream) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null value");
    }
    tableSpecCoder.encode(value.getTableSpec(), outStream);
    tableDescriptionCoder.encode(value.getTableDescription(), outStream);
  }

  @Override
  public TableDestination decode(InputStream inStream) throws IOException {
    String tableSpec = tableSpecCoder.decode(inStream);
    String tableDescription = tableDescriptionCoder.decode(inStream);
    return new TableDestination(tableSpec, tableDescription);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
