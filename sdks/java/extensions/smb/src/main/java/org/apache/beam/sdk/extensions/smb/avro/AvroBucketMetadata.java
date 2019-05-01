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
package org.apache.beam.sdk.extensions.smb.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;

/** Avro-specific metadata encoding. */
public class AvroBucketMetadata<SortingKeyT, ValueT extends GenericRecord>
    extends BucketMetadata<SortingKeyT, ValueT> {

  @JsonProperty private final String keyField;

  @JsonIgnore private final String[] keyPath;

  @JsonCreator
  public AvroBucketMetadata(
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("sortingKeyClass") Class<SortingKeyT> sortingKeyClass,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField) {
    super(numBuckets, sortingKeyClass, hashType);
    this.keyField = keyField;
    this.keyPath = keyField.split("\\.");
  }

  // @Todo: offer custom Avro coder types
  @Override
  public Coder<SortingKeyT> getSortingKeyCoder() throws CannotProvideCoderException {
    return CoderRegistry.createDefault().getCoder(getSortingKeyClass());
  }

  @SuppressWarnings("unchecked")
  @Override
  public SortingKeyT extractSortingKey(ValueT value) {
    GenericRecord node = value;
    for (int i = 0; i < keyPath.length - 1; i++) {
      node = (GenericRecord) node.get(keyPath[i]);
    }
    return (SortingKeyT) node.get(keyPath[keyPath.length - 1]);
  }

  // Coders for common Avro types
  private static class ByteBufferCoder extends AtomicCoder<ByteBuffer> {
    private static final ByteBufferCoder INSTANCE = new ByteBufferCoder();

    private ByteBufferCoder() {}

    public static ByteBufferCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(ByteBuffer value, OutputStream outStream)
        throws CoderException, IOException {
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      value.position(value.position() - bytes.length);

      ByteArrayCoder.of().encode(bytes, outStream);
    }

    @Override
    public ByteBuffer decode(InputStream inStream) throws CoderException, IOException {
      return ByteBuffer.wrap(ByteArrayCoder.of().decode(inStream));
    }
  }

  private static class CharSequenceCoder extends AtomicCoder<CharSequence> {
    private static final CharSequenceCoder INSTANCE = new CharSequenceCoder();

    private CharSequenceCoder() {}

    public static CharSequenceCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(CharSequence value, OutputStream outStream)
        throws CoderException, IOException {
      StringUtf8Coder.of().encode(value.toString(), outStream);
    }

    @Override
    public CharSequence decode(InputStream inStream) throws CoderException, IOException {
      return StringUtf8Coder.of().decode(inStream);
    }
  }
}
