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
package org.apache.beam.examples.webapis;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

// [START webapis_java_image_response_coder]

/** A {@link CustomCoder} of an {@link ImageResponse}. */
class ImageResponseCoder extends CustomCoder<ImageResponse> {
  public static ImageResponseCoder of() {
    return new ImageResponseCoder();
  }

  private static final Coder<byte[]> BYTE_ARRAY_CODER = ByteArrayCoder.of();
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

  @Override
  public void encode(ImageResponse value, OutputStream outStream)
      throws CoderException, IOException {
    BYTE_ARRAY_CODER.encode(value.getData().toByteArray(), outStream);
    STRING_CODER.encode(value.getMimeType(), outStream);
  }

  @Override
  public ImageResponse decode(InputStream inStream) throws CoderException, IOException {
    byte[] data = BYTE_ARRAY_CODER.decode(inStream);
    String mimeType = STRING_CODER.decode(inStream);
    return ImageResponse.builder().setData(ByteString.copyFrom(data)).setMimeType(mimeType).build();
  }
}

// [END webapis_java_image_response_coder]
