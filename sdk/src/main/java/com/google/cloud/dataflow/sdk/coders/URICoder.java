/*
 * Copyright (C) 2014 Google Inc.
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
import java.net.URI;
import java.net.URISyntaxException;

/**
 * A {@code URICoder} encodes/decodes {@link URI}s by conversion to/from {@link String}, delegating
 * encoding/decoding of the string to {@link StringUtf8Coder}.
 */
@SuppressWarnings("serial")
public class URICoder extends AtomicCoder<URI> {

  @JsonCreator
  public static URICoder of() {
    return INSTANCE;
  }

  private static final URICoder INSTANCE = new URICoder();
  private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();

  private URICoder() {}

  /////////////////////////////////////////////////////////////////////////////

  @Override
  public void encode(URI value, OutputStream outStream, Context context)
      throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null URI");
    }
    STRING_CODER.encode(value.toString(), outStream, context);
  }

  @Override
  public URI decode(InputStream inStream, Context context)
      throws IOException {
    try {
      return new URI(STRING_CODER.decode(inStream, context));
    } catch (URISyntaxException exn) {
      throw new CoderException(exn);
    }
  }

  @Override
  public boolean isDeterministic() {
    return STRING_CODER.isDeterministic();
  }

  @Override
  protected long getEncodedElementByteSize(URI value, Context context)
      throws Exception {
    if (value == null) {
      throw new CoderException("cannot encode a null URI");
    }
    return STRING_CODER.getEncodedElementByteSize(value.toString(), context);
  }
}
