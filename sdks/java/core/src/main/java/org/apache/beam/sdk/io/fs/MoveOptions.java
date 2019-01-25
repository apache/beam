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
package org.apache.beam.sdk.io.fs;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.FileSystems;

/**
 * An object that configures {@link FileSystems#copy}, {@link FileSystems#rename}, and {@link
 * FileSystems#delete}.
 */
public abstract class MoveOptions {

  /**
   * If false, operations will abort on errors arising from missing files. Otherwise, operations
   * will ignore such errors.
   */
  public abstract boolean getIgnoreMissingFiles();

  /**
   * Specifies the Key Management System key name to use to encrypt new files with.
   *
   * <p>Only relevant to operations that create new files and filesystems that support KMS features.
   */
  @Experimental(Kind.FILESYSTEM)
  @Nullable
  public abstract String getDestKmsKey();

  /** Defines the standard {@link MoveOptions}. */
  @AutoValue
  public abstract static class StandardMoveOptions extends MoveOptions {
    // This is a convenience member for backwards compatibility.
    public static final MoveOptions IGNORE_MISSING_FILES =
        StandardMoveOptions.builder().setIgnoreMissingFiles(true).build();

    @Override
    public abstract boolean getIgnoreMissingFiles();

    @Experimental(Kind.FILESYSTEM)
    @Override
    @Nullable
    public abstract String getDestKmsKey();

    public static Builder builder() {
      return new AutoValue_MoveOptions_StandardMoveOptions.Builder().setIgnoreMissingFiles(false);
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setIgnoreMissingFiles(boolean ignoreMissingFiles);

      @Experimental(Kind.FILESYSTEM)
      public abstract Builder setDestKmsKey(String kmsKey);

      public abstract StandardMoveOptions build();
    }
  }
}
