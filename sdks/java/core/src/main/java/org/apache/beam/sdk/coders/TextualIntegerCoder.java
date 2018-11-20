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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Coder} that encodes {@code Integer Integers} as the ASCII bytes of their textual,
 * decimal, representation.
 */
public class TextualIntegerCoder extends AtomicCoder<Integer> {

  public static TextualIntegerCoder of() {
    return new TextualIntegerCoder();
  }

  /////////////////////////////////////////////////////////////////////////////

  private static final TypeDescriptor<Integer> TYPE_DESCRIPTOR = new TypeDescriptor<Integer>() {};

  protected TextualIntegerCoder() {}

  @Override
  public void encode(Integer value, OutputStream outStream) throws IOException, CoderException {
    encode(value, outStream, Context.NESTED);
  }

  @Override
  public void encode(Integer value, OutputStream outStream, Context context)
      throws IOException, CoderException {
    if (value == null) {
      throw new CoderException("cannot encode a null Integer");
    }
    String textualValue = value.toString();
    StringUtf8Coder.of().encode(textualValue, outStream, context);
  }

  @Override
  public Integer decode(InputStream inStream) throws IOException, CoderException {
    return decode(inStream, Context.NESTED);
  }

  @Override
  public Integer decode(InputStream inStream, Context context) throws IOException, CoderException {
    String textualValue = StringUtf8Coder.of().decode(inStream, context);
    try {
      return Integer.valueOf(textualValue);
    } catch (NumberFormatException exn) {
      throw new CoderException("error when decoding a textual integer", exn);
    }
  }

  @Override
  public void verifyDeterministic() {
    StringUtf8Coder.of().verifyDeterministic();
  }

  @Override
  public TypeDescriptor<Integer> getEncodedTypeDescriptor() {
    return TYPE_DESCRIPTOR;
  }

  @Override
  protected long getEncodedElementByteSize(Integer value) throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null Integer");
    }
    String textualValue = value.toString();
    return StringUtf8Coder.of().getEncodedElementByteSize(textualValue);
  }
}
