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
package org.apache.beam.runners.dataflow.worker;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/** Native library used to read from and write to a shuffle dataset. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class ShuffleLibrary {
  /** Loads the native shuffle library. */
  static void load() {
    ShuffleLibraryLoader.load();
  }

  private static class ShuffleLibraryLoader {
    private static void load() {}

    static {
      try {
        final String shuffleClientLibraryPropertyKey = "batch.shuffle_client_library";
        String shuffleClientLibrary = "libshuffle_client_jni.so.stripped";
        if (System.getProperties().containsKey(shuffleClientLibraryPropertyKey)) {
          shuffleClientLibrary = System.getProperty(shuffleClientLibraryPropertyKey);
        }
        File tempfile = File.createTempFile("libshuffle_client_jni", ".so");
        InputStream input = ClassLoader.getSystemResourceAsStream(shuffleClientLibrary);
        Files.copy(input, tempfile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        System.load(tempfile.getAbsolutePath());
      } catch (IOException e) {
        throw new RuntimeException("Loading shuffle_client failed:", e);
      }
    }
  }
}
