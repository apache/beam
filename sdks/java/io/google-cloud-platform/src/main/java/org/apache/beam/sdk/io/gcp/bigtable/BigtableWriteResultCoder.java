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
package org.apache.beam.sdk.io.gcp.bigtable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A coder for {@link BigtableWriteResult}. */
public class BigtableWriteResultCoder extends AtomicCoder<BigtableWriteResult> {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  private static final BigtableWriteResultCoder INSTANCE = new BigtableWriteResultCoder();

  public static CoderProvider getCoderProvider() {
    return CoderProviders.forCoder(
        TypeDescriptor.of(BigtableWriteResult.class), BigtableWriteResultCoder.of());
  }

  public static BigtableWriteResultCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(BigtableWriteResult value, OutputStream outStream)
      throws CoderException, IOException {
    LONG_CODER.encode(value.getRowsWritten(), outStream);
  }

  @Override
  public BigtableWriteResult decode(InputStream inStream) throws CoderException, IOException {
    return BigtableWriteResult.create(LONG_CODER.decode(inStream));
  }
}
