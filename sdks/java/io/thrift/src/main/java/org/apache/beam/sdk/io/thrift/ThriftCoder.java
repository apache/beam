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
package org.apache.beam.sdk.io.thrift;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * A {@link org.apache.beam.sdk.coders.Coder} using a Thrift {@link TProtocol} to
 * serialize/deserialize elements.
 *
 * @param <T> type of element handled by coder.
 */
class ThriftCoder<T> extends CustomCoder<T> {

  private final Class<T> type;
  private final TProtocolFactory protocolFactory;

  protected ThriftCoder(Class<T> type, TProtocolFactory protocolFactory) {
    this.type = type;
    this.protocolFactory = protocolFactory;
  }

  /**
   * Returns an {@link ThriftCoder} instance for the provided {@code clazz} and {@code
   * protocolFactory}.
   *
   * @param clazz {@link TBase} class used to decode into/ encode from.
   * @param protocolFactory factory for {@link TProtocol} to be used to encode/decode.
   * @param <T> element type
   * @return ThriftCoder initialize with class to be encoded/decoded and {@link TProtocolFactory}
   *     used to encode/decode.
   */
  static <T> ThriftCoder<T> of(Class<T> clazz, TProtocolFactory protocolFactory) {
    return new ThriftCoder<>(clazz, protocolFactory);
  }

  /**
   * Encodes the given value of type {@code T} onto the given output stream using provided {@link
   * ThriftCoder#protocolFactory}.
   *
   * @param value {@link org.apache.thrift.TBase} to encode.
   * @param outStream stream to output encoded value to.
   * @throws IOException if writing to the {@code OutputStream} fails for some reason
   * @throws CoderException if the value could not be encoded for some reason
   */
  @Override
  public void encode(T value, OutputStream outStream) throws CoderException, IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(baos));
    try {
      TBase<?, ?> tBase = (TBase<?, ?>) value;
      tBase.write(protocol);
    } catch (Exception te) {
      throw new CoderException("Could not write value. Error: " + te.getMessage());
    }
    outStream.write(baos.toByteArray());
  }

  /**
   * Decodes a value of type {@code T} from the given input stream using provided {@link
   * ThriftCoder#protocolFactory}. Returns the decoded value.
   *
   * @param inStream stream of input values to be decoded
   * @throws IOException if reading from the {@code InputStream} fails for some reason
   * @throws CoderException if the value could not be decoded for some reason
   * @return {@link TBase} decoded object
   */
  @Override
  public T decode(InputStream inStream) throws CoderException, IOException {
    try {
      TProtocol protocol = protocolFactory.getProtocol(new TIOStreamTransport(inStream));
      TBase<?, ?> value = (TBase<?, ?>) type.getDeclaredConstructor().newInstance();
      value.read(protocol);
      return (T) value;
    } catch (Exception te) {
      throw new CoderException("Could not read value. Error: " + te.getMessage());
    }
  }
}
