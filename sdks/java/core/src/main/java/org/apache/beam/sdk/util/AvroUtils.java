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

import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;

/**
 * A set of utilities for working with Avro files.
 *
 * <p>These utilities are based on the <a
 * href="https://avro.apache.org/docs/1.7.7/spec.html">Avro 1.7.7</a> specification.
 */
public class AvroUtils {

  /**
   * Avro file metadata.
   */
  public static class AvroMetadata {
    private byte[] syncMarker;
    private String codec;
    private String schemaString;

    AvroMetadata(byte[] syncMarker, String codec, String schemaString) {
      this.syncMarker = syncMarker;
      this.codec = codec;
      this.schemaString = schemaString;
    }

    /**
     * The JSON-encoded <a href="https://avro.apache.org/docs/1.7.7/spec.html#schemas">schema</a>
     * string for the file.
     */
    public String getSchemaString() {
      return schemaString;
    }

    /**
     * The <a href="https://avro.apache.org/docs/1.7.7/spec.html#Required+Codecs">codec</a> of the
     * file.
     */
    public String getCodec() {
      return codec;
    }

    /**
     * The 16-byte sync marker for the file.  See the documentation for
     * <a href="https://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files">Object
     * Container File</a> for more information.
     */
    public byte[] getSyncMarker() {
      return syncMarker;
    }
  }

  /**
   * Reads the {@link AvroMetadata} from the header of an Avro file.
   *
   * <p>This method parses the header of an Avro
   * <a href="https://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files">
   * Object Container File</a>.
   *
   * @throws IOException if the file is an invalid format.
   */
  public static AvroMetadata readMetadataFromFile(String fileName) throws IOException {
    String codec = null;
    String schemaString = null;
    byte[] syncMarker;
    try (InputStream stream =
        Channels.newInputStream(IOChannelUtils.getFactory(fileName).open(fileName))) {
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(stream, null);

      // The header of an object container file begins with a four-byte magic number, followed
      // by the file metadata (including the schema and codec), encoded as a map. Finally, the
      // header ends with the file's 16-byte sync marker.
      // See https://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files for details on
      // the encoding of container files.

      // Read the magic number.
      byte[] magic = new byte[DataFileConstants.MAGIC.length];
      decoder.readFixed(magic);
      if (!Arrays.equals(magic, DataFileConstants.MAGIC)) {
        throw new IOException("Missing Avro file signature: " + fileName);
      }

      // Read the metadata to find the codec and schema.
      ByteBuffer valueBuffer = ByteBuffer.allocate(512);
      long numRecords = decoder.readMapStart();
      while (numRecords > 0) {
        for (long recordIndex = 0; recordIndex < numRecords; recordIndex++) {
          String key = decoder.readString();
          // readBytes() clears the buffer and returns a buffer where:
          // - position is the start of the bytes read
          // - limit is the end of the bytes read
          valueBuffer = decoder.readBytes(valueBuffer);
          byte[] bytes = new byte[valueBuffer.remaining()];
          valueBuffer.get(bytes);
          if (key.equals(DataFileConstants.CODEC)) {
            codec = new String(bytes, "UTF-8");
          } else if (key.equals(DataFileConstants.SCHEMA)) {
            schemaString = new String(bytes, "UTF-8");
          }
        }
        numRecords = decoder.mapNext();
      }
      if (codec == null) {
        codec = DataFileConstants.NULL_CODEC;
      }

      // Finally, read the sync marker.
      syncMarker = new byte[DataFileConstants.SYNC_SIZE];
      decoder.readFixed(syncMarker);
    }
    return new AvroMetadata(syncMarker, codec, schemaString);
  }
}
