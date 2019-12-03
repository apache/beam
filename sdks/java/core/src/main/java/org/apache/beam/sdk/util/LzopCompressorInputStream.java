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

import io.airlift.compress.lzo.LzopCodec;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.utils.CountingInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.compress.utils.InputStreamStatistics;

/**
 * {@link CompressorInputStream} implementation to create LZO encoded stream. Library relies on <a
 * href="https://github.com/airlift/aircompressor/">LZO</a>
 *
 * @since 1.18
 */
public class LzopCompressorInputStream extends CompressorInputStream
    implements InputStreamStatistics {

  private final CountingInputStream countingStream;
  private final InputStream lzoIS;

  /**
   * Wraps the given stream into a aircompressor's HadoopLzopInputStream using the LzopCodec.
   *
   * @param inStream the stream to write to
   * @throws IOException if aircompressor does
   */
  public LzopCompressorInputStream(final InputStream inStream) throws IOException {
    this.lzoIS =
        new LzopCodec().createInputStream(countingStream = new CountingInputStream(inStream));
  }

  @Override
  public int available() throws IOException {
    return lzoIS.available();
  }

  @Override
  public void close() throws IOException {
    lzoIS.close();
  }

  @Override
  public int read(final byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public long skip(final long n) throws IOException {
    return IOUtils.skip(lzoIS, n);
  }

  @Override
  public void mark(final int readlimit) {
    lzoIS.mark(readlimit);
  }

  @Override
  public boolean markSupported() {
    return lzoIS.markSupported();
  }

  @Override
  public int read() throws IOException {
    final int ret = lzoIS.read();
    count(ret == -1 ? 0 : 1);
    return ret;
  }

  @Override
  public int read(final byte[] buf, final int off, final int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    final int ret = lzoIS.read(buf, off, len);
    count(ret);
    return ret;
  }

  @Override
  public String toString() {
    return lzoIS.toString();
  }

  @Override
  public void reset() throws IOException {
    lzoIS.reset();
  }

  @Override
  public long getCompressedCount() {
    return countingStream.getBytesRead();
  }
}
