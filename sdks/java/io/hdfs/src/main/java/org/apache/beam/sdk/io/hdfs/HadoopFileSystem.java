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
package org.apache.beam.sdk.io.hdfs;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;

/**
 * Adapts {@link org.apache.hadoop.fs.FileSystem} connectors to be used as
 * Apache Beam {@link FileSystem FileSystems}.
 */
class HadoopFileSystem extends FileSystem<HadoopResourceId> {

  HadoopFileSystem() {}

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected WritableByteChannel create(HadoopResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected ReadableByteChannel open(HadoopResourceId resourceId) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void copy(
      List<HadoopResourceId> srcResourceIds,
      List<HadoopResourceId> destResourceIds) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void rename(
      List<HadoopResourceId> srcResourceIds,
      List<HadoopResourceId> destResourceIds) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void delete(Collection<HadoopResourceId> resourceIds) throws IOException {
    throw new UnsupportedOperationException();
  }
}
