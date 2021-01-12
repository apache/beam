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
package org.apache.beam.sdk.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.beam.sdk.util.LzoCompression;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Ints;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;

/** Various compression types for reading/writing files. */
@SuppressWarnings("ImmutableEnumChecker")
public enum Compression {
  /**
   * When reading a file, automatically determine the compression type based on filename extension.
   * Not applicable when writing files.
   */
  AUTO("") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) {
      throw new UnsupportedOperationException(
          "Must resolve compression into a concrete value before calling readDecompressed()");
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) {
      throw new UnsupportedOperationException("AUTO is applicable only to reading files");
    }
  },

  /** No compression. */
  UNCOMPRESSED("") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) {
      return channel;
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) {
      return channel;
    }
  },

  /** GZip compression. */
  GZIP(".gz", ".gz") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) throws IOException {
      // Determine if the input stream is gzipped. The input stream returned from the
      // GCS connector may already be decompressed; GCS does this based on the
      // content-encoding property.
      PushbackInputStream stream = new PushbackInputStream(Channels.newInputStream(channel), 2);
      byte[] headerBytes = new byte[2];
      int bytesRead =
          ByteStreams.read(
              stream /* source */, headerBytes /* dest */, 0 /* offset */, 2 /* len */);
      stream.unread(headerBytes, 0, bytesRead);
      if (bytesRead >= 2) {
        byte zero = 0x00;
        int header = Ints.fromBytes(zero, zero, headerBytes[1], headerBytes[0]);
        if (header == GZIPInputStream.GZIP_MAGIC) {
          return Channels.newChannel(new GzipCompressorInputStream(stream, true));
        }
      }
      return Channels.newChannel(stream);
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) throws IOException {
      return Channels.newChannel(new GZIPOutputStream(Channels.newOutputStream(channel), true));
    }
  },

  /** BZip compression. */
  BZIP2(".bz2", ".bz2") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) throws IOException {
      return Channels.newChannel(
          new BZip2CompressorInputStream(Channels.newInputStream(channel), true));
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) throws IOException {
      return Channels.newChannel(
          new BZip2CompressorOutputStream(Channels.newOutputStream(channel)));
    }
  },

  /** Zip compression. */
  ZIP(".zip", ".zip") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) throws IOException {
      FullZipInputStream zip = new FullZipInputStream(Channels.newInputStream(channel));
      return Channels.newChannel(zip);
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) throws IOException {
      throw new UnsupportedOperationException("Writing ZIP files is currently unsupported");
    }
  },

  /**
   * ZStandard compression.
   *
   * <p>The {@code .zst} extension is specified in <a href=https://tools.ietf.org/html/rfc8478>RFC
   * 8478</a>.
   *
   * <p>The Beam Java SDK does not pull in the Zstd library by default, so it is the user's
   * responsibility to declare an explicit dependency on {@code zstd-jni}. Attempts to read or write
   * .zst files without {@code zstd-jni} loaded will result in {@code NoClassDefFoundError} at
   * runtime.
   */
  ZSTD(".zst", ".zst", ".zstd") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) throws IOException {
      return Channels.newChannel(new ZstdCompressorInputStream(Channels.newInputStream(channel)));
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) throws IOException {
      return Channels.newChannel(new ZstdCompressorOutputStream(Channels.newOutputStream(channel)));
    }
  },

  /**
   * LZO compression using LZO codec. {@code .lzo_deflate} extension is specified for files which
   * use the LZO algorithm without headers.
   *
   * <p>The Beam Java SDK does not pull in the required libraries for LZO compression by default, so
   * it is the user's responsibility to declare an explicit dependency on {@code
   * io.airlift:aircompressor} and {@code com.facebook.presto.hadoop:hadoop-apache2}. Attempts to
   * read or write {@code .lzo_deflate} files without {@code io.airlift:aircompressor} and {@code
   * com.facebook.presto.hadoop:hadoop-apache2} loaded will result in a {@code NoClassDefFoundError}
   * at runtime.
   */
  LZO(".lzo_deflate", ".lzo_deflate") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) throws IOException {
      return Channels.newChannel(
          LzoCompression.createLzoInputStream(Channels.newInputStream(channel)));
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) throws IOException {
      return Channels.newChannel(
          LzoCompression.createLzoOutputStream(Channels.newOutputStream(channel)));
    }
  },

  /**
   * LZOP compression using LZOP codec. {@code .lzo} extension is specified for files with magic
   * bytes and headers.
   *
   * <p><b>Warning:</b> The LZOP codec being used does not support concatenated LZOP streams and
   * will silently ignore data after the end of the first LZOP stream.
   *
   * <p>The Beam Java SDK does not pull in the required libraries for LZOP compression by default,
   * so it is the user's responsibility to declare an explicit dependency on {@code
   * io.airlift:aircompressor} and {@code com.facebook.presto.hadoop:hadoop-apache2}. Attempts to
   * read or write {@code .lzo} files without {@code io.airlift:aircompressor} and {@code
   * com.facebook.presto.hadoop:hadoop-apache2} loaded will result in a {@code NoClassDefFoundError}
   * at runtime.
   */
  LZOP(".lzo", ".lzo") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) throws IOException {
      return Channels.newChannel(
          LzoCompression.createLzopInputStream(Channels.newInputStream(channel)));
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) throws IOException {
      return Channels.newChannel(
          LzoCompression.createLzopOutputStream(Channels.newOutputStream(channel)));
    }
  },

  /** Deflate compression. */
  DEFLATE(".deflate", ".deflate", ".zlib") {
    @Override
    public ReadableByteChannel readDecompressed(ReadableByteChannel channel) throws IOException {
      return Channels.newChannel(
          new DeflateCompressorInputStream(Channels.newInputStream(channel)));
    }

    @Override
    public WritableByteChannel writeCompressed(WritableByteChannel channel) throws IOException {
      return Channels.newChannel(
          new DeflateCompressorOutputStream(Channels.newOutputStream(channel)));
    }
  };

  private final String suggestedSuffix;
  private final ImmutableList<String> detectedSuffixes;

  Compression(String suggestedSuffix, String... detectedSuffixes) {
    this.suggestedSuffix = suggestedSuffix;
    this.detectedSuffixes = ImmutableList.copyOf(detectedSuffixes);
  }

  public String getSuggestedSuffix() {
    return suggestedSuffix;
  }

  public boolean matches(String filename) {
    for (String suffix : detectedSuffixes) {
      if (filename.toLowerCase().endsWith(suffix)) {
        return true;
      }
    }
    return false;
  }

  public boolean isCompressed(String filename) {
    Compression compression = this;
    if (compression == AUTO) {
      compression = detect(filename);
    }
    return compression != UNCOMPRESSED;
  }

  public static Compression detect(String filename) {
    for (Compression value : values()) {
      if (value.matches(filename)) {
        return value;
      }
    }
    return UNCOMPRESSED;
  }

  public abstract ReadableByteChannel readDecompressed(ReadableByteChannel channel)
      throws IOException;

  public abstract WritableByteChannel writeCompressed(WritableByteChannel channel)
      throws IOException;

  /** Concatenates all {@link ZipInputStream}s contained within the zip file. */
  private static class FullZipInputStream extends InputStream {
    private ZipInputStream zipInputStream;
    private ZipEntry currentEntry;

    public FullZipInputStream(InputStream is) throws IOException {
      super();
      zipInputStream = new ZipInputStream(is);
      currentEntry = zipInputStream.getNextEntry();
    }

    @Override
    public int read() throws IOException {
      int result = zipInputStream.read();
      while (result == -1) {
        currentEntry = zipInputStream.getNextEntry();
        if (currentEntry == null) {
          return -1;
        } else {
          result = zipInputStream.read();
        }
      }
      return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int result = zipInputStream.read(b, off, len);
      while (result == -1) {
        currentEntry = zipInputStream.getNextEntry();
        if (currentEntry == null) {
          return -1;
        } else {
          result = zipInputStream.read(b, off, len);
        }
      }
      return result;
    }
  }
}
