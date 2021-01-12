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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link Coder} that encodes BigQuery {@link TableRow} objects in their native JSON format. */
public class TableRowJsonCoder extends AtomicCoder<TableRow> {

  public static TableRowJsonCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableRow value, OutputStream outStream) throws IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(TableRow value, OutputStream outStream, Context context) throws IOException {
    String strValue = MAPPER.writeValueAsString(value);
    StringUtf8Coder.of().encode(strValue, outStream, context);
  }

  @Override
  public TableRow decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public TableRow decode(InputStream inStream, Context context) throws IOException {
    String strValue = StringUtf8Coder.of().decode(inStream, context);
    return MAPPER.readValue(strValue, TableRow.class);
  }

  @Override
  public long getEncodedElementByteSize(TableRow value) throws Exception {
    String strValue = MAPPER.writeValueAsString(value);
    return StringUtf8Coder.of().getEncodedElementByteSize(strValue);
  }

  /////////////////////////////////////////////////////////////////////////////

  // FAIL_ON_EMPTY_BEANS is disabled in order to handle null values in
  // TableRow.
  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private static final TableRowJsonCoder INSTANCE = new TableRowJsonCoder();
  private static final TypeDescriptor<TableRow> TYPE_DESCRIPTOR = new TypeDescriptor<TableRow>() {};

  private TableRowJsonCoder() {}

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always. A {@link TableRow} can hold arbitrary {@link Object}
   *     instances, which makes the encoding non-deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this, "TableCell can hold arbitrary instances, which may be non-deterministic.");
  }

  @Override
  public TypeDescriptor<TableRow> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
