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
package org.apache.beam.sdk.io.solr;

import com.google.auto.service.AutoService;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderProviderRegistrar;
import org.apache.beam.sdk.coders.CoderProviders;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.compress.utils.BoundedInputStream;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.JavaBinCodec;

/** A {@link Coder} that encodes using {@link JavaBinCodec}. */
class JavaBinCodecCoder<T> extends AtomicCoder<T> {
  private final Class<T> clazz;

  private JavaBinCodecCoder(Class<T> clazz) {
    this.clazz = clazz;
  }

  public static <T> JavaBinCodecCoder<T> of(Class<T> clazz) {
    return new JavaBinCodecCoder<>(clazz);
  }

  @Override
  public void encode(T value, OutputStream outStream) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null SolrDocument");
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JavaBinCodec codec = new JavaBinCodec();
    codec.marshal(value, baos);

    byte[] bytes = baos.toByteArray();
    VarInt.encode(bytes.length, outStream);
    outStream.write(bytes);
  }

  @Override
  public T decode(InputStream inStream) throws IOException {
    DataInputStream in = new DataInputStream(inStream);

    int len = VarInt.decodeInt(in);
    if (len < 0) {
      throw new CoderException("Invalid encoded SolrDocument length: " + len);
    }

    JavaBinCodec codec = new JavaBinCodec();
    return (T) codec.unmarshal(new BoundedInputStream(in, len));
  }

  @Override
  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return TypeDescriptor.of(clazz);
  }

  @AutoService(CoderProviderRegistrar.class)
  public static class Provider implements CoderProviderRegistrar {
    @Override
    public List<CoderProvider> getCoderProviders() {
      return Arrays.asList(
          CoderProviders.forCoder(
              TypeDescriptor.of(SolrDocument.class), JavaBinCodecCoder.of(SolrDocument.class)),
          CoderProviders.forCoder(
              TypeDescriptor.of(SolrInputDocument.class),
              JavaBinCodecCoder.of(SolrInputDocument.class)));
    }
  }
}
