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

import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

// [START webapis_java_image_response]

/** An HTTP response of an image request. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class ImageResponse implements Serializable {

  static Builder builder() {
    return new AutoValue_ImageResponse.Builder();
  }

  /** The MIME type of the response payload. */
  abstract String getMimeType();

  /** The payload of the response containing the image data. */
  abstract ByteString getData();

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setMimeType(String value);

    abstract Builder setData(ByteString value);

    abstract ImageResponse build();
  }
}

// [END webapis_java_image_response]
