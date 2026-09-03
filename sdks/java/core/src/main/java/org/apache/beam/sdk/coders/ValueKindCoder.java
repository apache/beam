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
package org.apache.beam.sdk.coders;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.Elements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.sdk.values.ValueKindUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Coder} for {@link ValueKind}, encoded in 1 byte as the matching {@link
 * Elements.ValueKind.Enum} number, so the wire format stays stable if the
 * enum is reordered and matches the portability representation.
 */
public class ValueKindCoder extends AtomicCoder<ValueKind> {

  public static ValueKindCoder of() {
    return INSTANCE;
  }

  private static final ValueKindCoder INSTANCE = new ValueKindCoder();
  private static final TypeDescriptor<ValueKind> TYPE_DESCRIPTOR =
      TypeDescriptor.of(ValueKind.class);

  private ValueKindCoder() {}

  @Override
  public void encode(ValueKind value, OutputStream outStream) throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null ValueKind");
    }
    outStream.write(ValueKindUtil.toProto(value).getNumber());
  }

  @Override
  public ValueKind decode(InputStream inStream) throws IOException, CoderException {
    int number = inStream.read();
    if (number == -1) {
      throw new CoderException(new EOFException("EOF encountered decoding a ValueKind"));
    }
    Elements.ValueKind.@Nullable Enum proto = Elements.ValueKind.Enum.forNumber(number);
    if (proto == null) {
      throw new CoderException("Unknown ValueKind number: " + number);
    }

    return ValueKindUtil.fromProto(proto);
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(ValueKind value) {
    return true;
  }

  @Override
  protected long getEncodedElementByteSize(ValueKind value) {
    return 1;
  }

  @Override
  public TypeDescriptor<ValueKind> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }
}
