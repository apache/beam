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
package org.apache.beam.sdk.extensions.euphoria.core.executor.io;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

/** Factory for creating files for spilling. */
@Audience(Audience.Type.EXECUTOR)
@FunctionalInterface
public interface SpillFileFactory extends Serializable {

  /**
   * Invoked to request a unique path to a new spill file.
   *
   * @return the path to a new spill file
   */
  File newSpillFile();

  /** The default spill file factory to create new files in the current working directory. */
  class DefaultSpillFileFactory implements SpillFileFactory {

    private static final SpillFileFactory INSTANCE = new DefaultSpillFileFactory();

    private DefaultSpillFileFactory() {}

    public static SpillFileFactory getInstance() {
      return INSTANCE;
    }

    @Override
    public File newSpillFile() {
      try {
        Path workDir = FileSystems.getFileSystem(new URI("file:///")).getPath("./");
        return Files.createTempFile(workDir, getClass().getName(), ".dat").toFile();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
