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

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class BigQueryStorageApiInsertErrorCoder extends AtomicCoder<BigQueryStorageApiInsertError> {
  static final Coder<TableRow> TABLE_ROW_CODER = TableRowJsonCoder.of();
  static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  static final BigQueryStorageApiInsertErrorCoder INSTANCE =
      new BigQueryStorageApiInsertErrorCoder();

  public static Coder<BigQueryStorageApiInsertError> of() {
    return INSTANCE;
  }

  @Override
  public void encode(BigQueryStorageApiInsertError value, OutputStream outStream)
      throws IOException {
    TABLE_ROW_CODER.encode(value.getRow(), outStream);
    STRING_CODER.encode(value.getErrorMessage(), outStream);
  }

  @Override
  public BigQueryStorageApiInsertError decode(InputStream inStream)
      throws CoderException, IOException {
    return new BigQueryStorageApiInsertError(
        TABLE_ROW_CODER.decode(inStream), STRING_CODER.decode(inStream));
  }
}
