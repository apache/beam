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
package org.apache.beam.sdk.coders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Coder} that encodes BigQuery {@link TableSchema} objects in their native JSON format.
 */
public class TableSchemaJsonCoder extends AtomicCoder<TableSchema> {

  @JsonCreator
  public static TableSchemaJsonCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(TableSchema value, OutputStream outStream, Context context)
      throws IOException {
    String strValue = MAPPER.writeValueAsString(value);
    StringUtf8Coder.of().encode(strValue, outStream, context);
  }

  @Override
  public TableSchema decode(InputStream inStream, Context context)
      throws IOException {
    String strValue = StringUtf8Coder.of().decode(inStream, context);
    return MAPPER.readValue(strValue, TableSchema.class);
  }

  @Override
  protected long getEncodedElementByteSize(TableSchema value, Context context)
      throws Exception {
    String strValue = MAPPER.writeValueAsString(value);
    return StringUtf8Coder.of().getEncodedElementByteSize(strValue, context);
  }

  /////////////////////////////////////////////////////////////////////////////

  // FAIL_ON_EMPTY_BEANS is disabled in order to handle null values in
  // TableSchema.
  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private static final TableSchemaJsonCoder INSTANCE = new TableSchemaJsonCoder();
  private static final TypeDescriptor<TableSchema> TYPE_DESCRIPTOR =
      new TypeDescriptor<TableSchema>() {};

  private TableSchemaJsonCoder() { }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always. A {@link TableSchema} can hold arbitrary
   *         {@link Object} instances, which makes the encoding non-deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "TableCell can hold arbitrary instances, which may be non-deterministic.");
  }

  @Override
  public TypeDescriptor<TableSchema> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
