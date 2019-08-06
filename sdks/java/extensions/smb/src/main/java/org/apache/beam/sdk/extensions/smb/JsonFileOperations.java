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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.util.MimeTypes;

/**
 * {@link org.apache.beam.sdk.extensions.smb.FileOperations} implementation for text files with
 * BigQuery {@link TableRow} JSON records.
 */
class JsonFileOperations extends FileOperations<TableRow> {
  private JsonFileOperations(Compression compression) {
    super(compression, compression == Compression.UNCOMPRESSED ? MimeTypes.TEXT : MimeTypes.BINARY);
  }

  public static JsonFileOperations of(Compression compression) {
    return new JsonFileOperations(compression);
  }

  @Override
  protected Reader<TableRow> createReader() {
    return new JsonReader();
  }

  @Override
  protected FileIO.Sink<TableRow> createSink() {
    return new FileIO.Sink<TableRow>() {

      private final ObjectMapper objectMapper = new ObjectMapper();
      private final FileIO.Sink<String> sink = TextIO.sink();

      @Override
      public void open(WritableByteChannel channel) throws IOException {
        sink.open(channel);
      }

      @Override
      public void write(TableRow element) throws IOException {
        sink.write(objectMapper.writeValueAsString(element));
      }

      @Override
      public void flush() throws IOException {
        sink.flush();
      }
    };
  }

  @Override
  public Coder<TableRow> getCoder() {
    return TableRowJsonCoder.of();
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class JsonReader extends FileOperations.Reader<TableRow> {

    private transient ObjectMapper objectMapper;
    private transient BufferedReader reader;
    private String next;

    @Override
    public void prepareRead(ReadableByteChannel channel) throws IOException {
      objectMapper = new ObjectMapper();
      reader =
          new BufferedReader(
              new InputStreamReader(Channels.newInputStream(channel), Charset.defaultCharset()));
    }

    @Override
    TableRow readNext() throws IOException, NoSuchElementException {
      if (next == null) {
        throw new NoSuchElementException();
      }
      return objectMapper.readValue(next, TableRow.class);
    }

    @Override
    boolean hasNextElement() throws IOException {
      next = reader.readLine();
      return next != null;
    }

    @Override
    public void finishRead() throws IOException {
      reader.close();
    }
  }
}
