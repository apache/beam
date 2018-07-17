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

package org.apache.beam.sdk.nexmark.model.sql;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

/**
 * {@link KnownSize} implementation to estimate the size of a {@link Row}, similar to Java model.
 * NexmarkLauncher/Queries infrastructure expects the events to be able to quickly provide the
 * estimates of their sizes.
 *
 * <p>The {@link Row} size is calculated at creation time.
 */
public class RowSize implements KnownSize {
  private static final Coder<Long> LONG_CODER = VarLongCoder.of();
  public static final Coder<RowSize> CODER =
      new CustomCoder<RowSize>() {
        @Override
        public void encode(RowSize rowSize, OutputStream outStream) throws IOException {

          LONG_CODER.encode(rowSize.sizeInBytes(), outStream);
        }

        @Override
        public RowSize decode(InputStream inStream) throws IOException {
          return new RowSize(LONG_CODER.decode(inStream));
        }
      };

  public static ParDo.SingleOutput<Row, RowSize> parDo() {
    return ParDo.of(
        new DoFn<Row, RowSize>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(RowSize.of(c.element()));
          }
        });
  }

  public static RowSize of(Row row) {
    return new RowSize(sizeInBytes(row));
  }

  private static long sizeInBytes(Row row) {
    return RowCoder.estimatedSizeBytes(row);
  }

  private long sizeInBytes;

  private RowSize(long sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }
}
