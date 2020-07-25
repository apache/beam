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
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** Defines a coder for {@link TableRowInfo} objects. */
@VisibleForTesting
class TableRowInfoCoder<ElementT> extends AtomicCoder<TableRowInfo<ElementT>> {
  private final Coder<ElementT> elementCoder;

  private TableRowInfoCoder(Coder<ElementT> elementCoder) {
    this.elementCoder = elementCoder;
  }

  public static <ElementT> TableRowInfoCoder<ElementT> of(Coder<ElementT> elementCoder) {
    return new TableRowInfoCoder<>(elementCoder);
  }

  @Override
  public void encode(TableRowInfo<ElementT> value, OutputStream outStream) throws IOException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(TableRowInfo<ElementT> value, OutputStream outStream, Context context)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null value");
    }
    elementCoder.encode(value.tableRow, outStream);
    idCoder.encode(value.uniqueId, outStream, context);
  }

  @Override
  public TableRowInfo<ElementT> decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public TableRowInfo<ElementT> decode(InputStream inStream, Context context) throws IOException {
    return new TableRowInfo<>(elementCoder.decode(inStream), idCoder.decode(inStream, context));
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this, "TableRows are not deterministic.");
  }

  StringUtf8Coder idCoder = StringUtf8Coder.of();
}
