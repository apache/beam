/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.coders;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A {@link Coder} that encodes {@code Integer Integers} as the ASCII bytes of
 * their textual, decimal, representation.
 */
public class TextualIntegerCoder extends AtomicCoder<Integer> {

  @JsonCreator
  public static TextualIntegerCoder of() {
    return new TextualIntegerCoder();
  }

  /////////////////////////////////////////////////////////////////////////////

  protected TextualIntegerCoder() {}

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
  public Integer decode(InputStream inStream, Context context)
      throws IOException, CoderException {
    String textualValue = StringUtf8Coder.of().decode(inStream, context);
    try {
      return Integer.valueOf(textualValue);
    } catch (NumberFormatException exn) {
      throw new CoderException("error when decoding a textual integer", exn);
    }
  }

  @Override
  protected long getEncodedElementByteSize(Integer value, Context context) throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null Integer");
    }
    String textualValue = value.toString();
    return StringUtf8Coder.of().getEncodedElementByteSize(textualValue, context);
  }
}
