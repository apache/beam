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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Implements IOChannelFactory for GCS.
 */
public class GcsIOChannelFactory implements IOChannelFactory {

  private final GcsOptions options;

  public GcsIOChannelFactory(GcsOptions options) {
    this.options = options;
  }

  @Override
  public Collection<String> match(String spec) throws IOException {
    GcsPath path = GcsPath.fromUri(spec);
    GcsUtil util = options.getGcsUtil();
    List<GcsPath> matched = util.expand(path);

    List<String> specs = new LinkedList<>();
    for (GcsPath match : matched) {
      specs.add(match.toString());
    }

    return specs;
  }

  @Override
  public ReadableByteChannel open(String spec) throws IOException {
    GcsPath path = GcsPath.fromUri(spec);
    GcsUtil util = options.getGcsUtil();
    return util.open(path);
  }

  @Override
  public WritableByteChannel create(String spec, String mimeType)
      throws IOException {
    GcsPath path = GcsPath.fromUri(spec);
    GcsUtil util = options.getGcsUtil();
    return util.create(path, mimeType);
  }

  @Override
  public long getSizeBytes(String spec) throws IOException {
    GcsPath path = GcsPath.fromUri(spec);
    GcsUtil util = options.getGcsUtil();
    return util.fileSize(path);
  }

  @Override
  public boolean isReadSeekEfficient(String spec) throws IOException {
    // TODO It is incorrect to return true here for files with content encoding set to gzip.
    return true;
  }

  @Override
  public String resolve(String path, String other) throws IOException {
    return GcsPath.fromUri(path).resolve(other).toString();
  }
}
