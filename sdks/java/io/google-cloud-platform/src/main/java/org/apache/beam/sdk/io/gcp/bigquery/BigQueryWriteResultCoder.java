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
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** A {@link Coder} that encodes BigQuery {@link BigQueryWriteResult} objects. */
public class BigQueryWriteResultCoder extends AtomicCoder<BigQueryWriteResult> {

  private static final BigQueryWriteResultCoder INSTANCE = new BigQueryWriteResultCoder();

  public static BigQueryWriteResultCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(BigQueryWriteResult value, OutputStream outStream) throws IOException {
    StringUtf8Coder.of().encode(value.getStatus().name(), outStream);
    StringUtf8Coder.of().encode(value.getTable(), outStream);
  }

  @Override
  public BigQueryWriteResult decode(InputStream inStream) throws IOException {
    return new BigQueryWriteResult(
        BigQueryHelpers.Status.valueOf(StringUtf8Coder.of().decode(inStream)),
        StringUtf8Coder.of().decode(inStream));
  }
}
