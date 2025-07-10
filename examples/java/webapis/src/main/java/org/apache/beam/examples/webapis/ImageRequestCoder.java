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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

/** A {@link CustomCoder} of an {@link ImageRequest}. */
class ImageRequestCoder extends CustomCoder<ImageRequest> {
  private static final StringUtf8Coder STRING_UTF_8_CODER = StringUtf8Coder.of();

  static ImageRequestCoder of() {
    return new ImageRequestCoder();
  }

  @Override
  public void encode(ImageRequest value, OutputStream outStream)
      throws CoderException, IOException {
    STRING_UTF_8_CODER.encode(value.getImageUrl(), outStream);
    STRING_UTF_8_CODER.encode(value.getMimeType(), outStream);
  }

  @Override
  public ImageRequest decode(InputStream inStream) throws CoderException, IOException {
    String imageUrl = STRING_UTF_8_CODER.decode(inStream);
    String mimeType = STRING_UTF_8_CODER.decode(inStream);
    return ImageRequest.builder().setImageUrl(imageUrl).setMimeType(mimeType).build();
  }
}
