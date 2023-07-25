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
package org.apache.beam.sdk.extensions.sbe;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.beam.sdk.coders.VarIntCoder;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.ir.IrEncoder;

/** A wrapper around {@link Ir} that fulfils Java's {@link Serializable} contract. */
public final class SerializableIr implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final int SCHEMA_BUFFER_CAPACITY = 16 * 1024; // 16 KB

  private static final VarIntCoder LEN_CODER = VarIntCoder.of();

  private Ir ir;

  private SerializableIr(Ir ir) {
    this.ir = ir;
  }

  /**
   * Creates a new instance from {@code ir}.
   *
   * @param ir the {@link Ir} to create the instance from
   * @return a new {@link SerializableIr} that contains {@code ir}
   */
  public static SerializableIr fromIr(Ir ir) {
    return new SerializableIr(ir);
  }

  /**
   * Returns the underlying {@link Ir}.
   *
   * <p>Modifications made to the returned value, such as through {@link Ir#addMessage(long, List)},
   * will be reflected in this instance.
   *
   * @return the underlying {@link Ir}
   */
  public Ir ir() {
    return ir;
  }

  // Serializable implementation

  private void writeObject(ObjectOutputStream out) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(SCHEMA_BUFFER_CAPACITY);
    int encodedBytes;
    try (IrEncoder encoder = new IrEncoder(buffer, ir)) {
      encodedBytes = encoder.encode();
    }

    // Encoding the number of bytes that this is to make decoding a little easier.
    LEN_CODER.encode(encodedBytes, out);
    for (int i = 0; i < encodedBytes; ++i) {
      out.writeByte(buffer.get(i));
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    int len = LEN_CODER.decode(in);
    byte[] bytes = new byte[len];
    for (int i = 0; i < len; ++i) {
      bytes[i] = in.readByte();
    }

    try (IrDecoder decoder = new IrDecoder(ByteBuffer.wrap(bytes))) {
      ir = decoder.decode();
    }
  }
}
