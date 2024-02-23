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
package org.apache.beam.examples.snippets.transforms.io.webapis;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.utils.FileNameUtils;

// [START webapis_image_request]

/** An HTTP request for an image. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ImageRequest implements Serializable {
  private static final Map<String, String> EXT_MIMETYPE_MAP =
      ImmutableMap.of(
          "jpg", "image/jpeg",
          "jpeg", "image/jpeg",
          "png", "image/png");

  /** Derive the MIME type of the image from the url based on its extension. */
  private static String mimeTypeOf(String url) {
    String ext = FileNameUtils.getExtension(url);
    if (!EXT_MIMETYPE_MAP.containsKey(ext)) {
      throw new IllegalArgumentException(
          String.format("could not map extension to mimetype: ext %s of url: %s", ext, url));
    }
    return EXT_MIMETYPE_MAP.get(ext);
  }

  public static Builder builder() {
    return new AutoValue_ImageRequest.Builder();
  }

  /** Build an {@link ImageRequest} from a {@param url}. */
  public static ImageRequest of(String url) {
    return builder().setImageUrl(url).setMimeType(mimeTypeOf(url)).build();
  }

  /** The URL of the image request. */
  public abstract String getImageUrl();

  /** The MIME type of the image request. */
  public abstract String getMimeType();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setImageUrl(String value);

    public abstract Builder setMimeType(String value);

    public abstract ImageRequest build();
  }
}

// [END webapis_image_request]
