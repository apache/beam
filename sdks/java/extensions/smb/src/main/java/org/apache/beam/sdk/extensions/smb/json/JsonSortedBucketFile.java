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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile;

public class JsonSortedBucketFile extends SortedBucketFile<Map<String, Object>> {
  @Override
  public Reader<Map<String, Object>> createReader() {
    return null;
  }

  @Override
  public Writer<Map<String, Object>> createWriter() {
    return null;
  }

  ////////////////////////////////////////
  // Reader
  ////////////////////////////////////////

  private static class JsonReader extends SortedBucketFile.Reader<Map<String, Object>> {

    private transient ObjectMapper objectMapper;
    private transient InputStream inputStream;

    @Override
    public void prepareRead(ReadableByteChannel channel) throws Exception {
      objectMapper = new ObjectMapper();
      inputStream = Channels.newInputStream(channel);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> read() throws Exception {
      return (Map<String, Object>) objectMapper.readValue(inputStream, Map.class);
    }

    @Override
    public void finishRead() throws Exception {
      inputStream.close();
    }
  }

  ////////////////////////////////////////
  // Writer
  ////////////////////////////////////////

  private static class JsonWriter extends SortedBucketFile.Writer<Map<String, Object>> {

    private transient ObjectMapper objectMapper;
    private transient OutputStream outputStream;

    @Override
    public String getMimeType() {
      return "application/json";
    }

    @Override
    public void prepareWrite(WritableByteChannel channel) throws Exception {
      objectMapper = new ObjectMapper();
      outputStream = Channels.newOutputStream(channel);
    }

    @Override
    public void write(Map<String, Object> value) throws Exception {
      objectMapper.writeValue(outputStream, value);
    }

    @Override
    public void finishWrite() throws Exception {
      outputStream.close();
    }
  }
}
