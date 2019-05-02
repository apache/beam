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
package org.apache.beam.sdk.extensions.smb.json;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.extensions.smb.FileOperations;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class JsonFileOperations extends FileOperations<TableRow> {
  @Override
  public Reader<TableRow> createReader() {
    return new JsonReader();
  }

  @Override
  public Writer<TableRow> createWriter() {
    return new JsonWriter();
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class JsonReader extends FileOperations.Reader<TableRow> {

    private transient TableRowJsonCoder tableRowJsonCoder;
    private transient InputStream inputStream;

    @Override
    public void prepareRead(ReadableByteChannel channel) throws Exception {
      tableRowJsonCoder = TableRowJsonCoder.of();
      inputStream = Channels.newInputStream(channel);
    }

    @Override
    public TableRow read() throws Exception {
      return tableRowJsonCoder.decode(inputStream);
    }

    @Override
    public void finishRead() throws Exception {
      inputStream.close();
    }
  }

  ////////////////////////////////////////
  // Writer
  ////////////////////////////////////////

  private static class JsonWriter extends FileOperations.Writer<TableRow> {

    private transient TableRowJsonCoder tableRowJsonCoder;
    private transient OutputStream outputStream;

    @Override
    public String getMimeType() {
      return "application/json";
    }

    @Override
    public void prepareWrite(WritableByteChannel channel) throws Exception {
      tableRowJsonCoder = TableRowJsonCoder.of();
      outputStream = Channels.newOutputStream(channel);
    }

    @Override
    public void write(TableRow value) throws Exception {
      tableRowJsonCoder.encode(value, outputStream);
    }

    @Override
    public void finishWrite() throws Exception {
      outputStream.close();
    }
  }
}
