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
package org.apache.beam.sdk.extensions.smb;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.util.MimeTypes;

class TestFileOperations extends FileOperations<String> {

  TestFileOperations() {
    super(Compression.UNCOMPRESSED, MimeTypes.TEXT);
  }

  @Override
  public Reader<String> createReader() {
    return new Reader<String>() {
      private transient BufferedReader reader;

      @Override
      public void prepareRead(ReadableByteChannel channel) throws Exception {
        reader =
            new BufferedReader(
                new InputStreamReader(Channels.newInputStream(channel), Charset.defaultCharset()));
      }

      @Override
      public String read() throws Exception {
        return reader.readLine();
      }

      @Override
      public void finishRead() throws Exception {
        reader.close();
      }
    };
  }

  @Override
  public FileIO.Sink<String> createSink() {
    return TextIO.sink();
  }

  @Override
  public Coder<String> getCoder() {
    return StringUtf8Coder.of();
  }
}
