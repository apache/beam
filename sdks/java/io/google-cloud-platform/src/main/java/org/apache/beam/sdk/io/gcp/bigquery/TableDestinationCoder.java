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

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** A coder for {@link TableDestination} objects. */
public class TableDestinationCoder extends AtomicCoder<TableDestination> {
  private static final TableDestinationCoder INSTANCE = new TableDestinationCoder();
  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();

  @JsonCreator
  public static TableDestinationCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableDestination value, OutputStream outStream, Context context)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null value");
    }
    stringCoder.encode(value.getTableSpec(), outStream, context.nested());
    stringCoder.encode(value.getTableDescription(), outStream, context);
  }

  @Override
  public TableDestination decode(InputStream inStream, Context context) throws IOException {
    return new TableDestination(
        stringCoder.decode(inStream, context.nested()),
        stringCoder.decode(inStream, context.nested()));
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    return;
  }
}
