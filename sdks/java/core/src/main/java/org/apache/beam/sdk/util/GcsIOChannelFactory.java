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
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.gcsfs.GcsPath;

/**
 * Implements IOChannelFactory for GCS.
 */
public class GcsIOChannelFactory implements IOChannelFactory {

  /**
   * Create a {@link GcsIOChannelFactory} with the given {@link PipelineOptions}.
   */
  public static GcsIOChannelFactory fromOptions(PipelineOptions options) {
    return new GcsIOChannelFactory(options.as(GcsOptions.class));
  }

  private final GcsOptions options;

  private GcsIOChannelFactory(GcsOptions options) {
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
    return toPath(path).resolve(other).toString();
  }

  @Override
  public Path toPath(String path) {
    return GcsPath.fromUri(path);
  }

  @Override
  public void copy(Iterable<String> srcFilenames, Iterable<String> destFilenames)
      throws IOException {
    options.getGcsUtil().copy(srcFilenames, destFilenames);
  }

  @Override
  public void remove(Collection<String> filesOrDirs) throws IOException {
    options.getGcsUtil().remove(filesOrDirs);
  }
}
