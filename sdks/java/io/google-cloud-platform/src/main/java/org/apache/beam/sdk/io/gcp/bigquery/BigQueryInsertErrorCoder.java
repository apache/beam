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
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A {@link Coder} that encodes BigQuery {@link BigQueryInsertError} objects. */
public class BigQueryInsertErrorCoder extends AtomicCoder<BigQueryInsertError> {

  public static BigQueryInsertErrorCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(BigQueryInsertError value, OutputStream outStream) throws IOException {
    String errorStrValue = MAPPER.writeValueAsString(value.getError());
    StringUtf8Coder.of().encode(errorStrValue, outStream);

    TableRowJsonCoder.of().encode(value.getRow(), outStream);

    StringUtf8Coder.of().encode(BigQueryHelpers.toTableSpec(value.getTable()), outStream);
  }

  @Override
  public BigQueryInsertError decode(InputStream inStream) throws IOException {
    TableDataInsertAllResponse.InsertErrors err =
        MAPPER.readValue(
            StringUtf8Coder.of().decode(inStream), TableDataInsertAllResponse.InsertErrors.class);
    TableRow row = TableRowJsonCoder.of().decode(inStream);
    TableReference ref = BigQueryHelpers.parseTableSpec(StringUtf8Coder.of().decode(inStream));
    return new BigQueryInsertError(row, err, ref);
  }

  @Override
  protected long getEncodedElementByteSize(BigQueryInsertError value) throws Exception {
    String errorStrValue = MAPPER.writeValueAsString(value.getError());
    String tableStrValue = MAPPER.writeValueAsString(value.getTable());
    return StringUtf8Coder.of().getEncodedElementByteSize(errorStrValue)
        + TableRowJsonCoder.of().getEncodedElementByteSize(value.getRow())
        + StringUtf8Coder.of().getEncodedElementByteSize(tableStrValue);
  }

  /////////////////////////////////////////////////////////////////////////////

  // FAIL_ON_EMPTY_BEANS is disabled in order to handle null values in BigQueryInsertError.
  // NON_FINAL default typing is enabled so that the java.lang.Long value on the errors index can be
  // properly deserialized as such and not as an Integer instead.
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
          .enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL);

  private static final BigQueryInsertErrorCoder INSTANCE = new BigQueryInsertErrorCoder();
  private static final TypeDescriptor<BigQueryInsertError> TYPE_DESCRIPTOR =
      new TypeDescriptor<BigQueryInsertError>() {};

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always. A {@link TableRow} can hold arbitrary {@link Object}
   *     instances, which makes the encoding non-deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this, "TableRow can hold arbitrary instances, which may be non-deterministic.");
  }

  @Override
  public TypeDescriptor<BigQueryInsertError> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
