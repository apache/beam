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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.VarInt;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.JavaBinCodec;

/**
 * A {@link Coder} that encodes {@link SolrDocument SolrDocument}.
 */
class SolrDocumentCoder extends AtomicCoder<SolrDocument> {

  private static final SolrDocumentCoder INSTANCE = new SolrDocumentCoder();

  public static SolrDocumentCoder of() {
    return INSTANCE;
  }

  @Override public void encode(SolrDocument value, OutputStream outStream)
      throws CoderException, IOException {
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

  @Override public SolrDocument decode(InputStream inStream) throws CoderException, IOException {
    DataInputStream in = new DataInputStream(inStream);

    int len = VarInt.decodeInt(in);
    if (len < 0) {
      throw new CoderException("Invalid encoded SolrDocument length: " + len);
    }
    byte[] bytes = new byte[len];
    in.readFully(bytes);

    JavaBinCodec codec = new JavaBinCodec();
    return (SolrDocument) codec.unmarshal(new ByteArrayInputStream(bytes));
  }
}
