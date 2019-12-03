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
package org.apache.beam.sdk.util;

import io.airlift.compress.lzo.LzoCodec;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.commons.compress.compressors.CompressorOutputStream;

/**
 * {@link CompressorOutputStream} implementation to create LZO encoded stream. Library relies on <a
 * href="https://github.com/airlift/aircompressor/">LZO</a>
 *
 * @since 1.18
 */
public class LzoCompressorOutputStream extends CompressorOutputStream {

  private final OutputStream lzoOS;

  /**
   * Wraps the given stream into a aircompressor's HadoopLzoOutputStream using the LzoCodec.
   *
   * @param outStream the stream to write to
   * @throws IOException if aircompressor does
   */
  public LzoCompressorOutputStream(final OutputStream outStream) throws IOException {
    this.lzoOS = new LzoCodec().createOutputStream(outStream);
  }

  @Override
  public void close() throws IOException {
    lzoOS.close();
  }

  @Override
  public void write(final int b) throws IOException {
    lzoOS.write(b);
  }

  @Override
  public void write(final byte[] b) throws IOException {
    lzoOS.write(b);
  }

  @Override
  public void write(final byte[] buf, final int off, final int len) throws IOException {
    lzoOS.write(buf, off, len);
  }

  @Override
  public String toString() {
    return lzoOS.toString();
  }

  @Override
  public void flush() throws IOException {
    lzoOS.flush();
  }
}
