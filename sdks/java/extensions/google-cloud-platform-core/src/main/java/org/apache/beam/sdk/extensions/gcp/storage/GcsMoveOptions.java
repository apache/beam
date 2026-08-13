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
package org.apache.beam.sdk.extensions.gcp.storage;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.fs.MoveOptions;

/** Google Cloud Storage-specific options for moving resources. */
@AutoValue
public abstract class GcsMoveOptions implements MoveOptions {

  /** The Cloud KMS key to use to encrypt destination objects. */
  public abstract String destinationKmsKeyName();

  /** Returns a {@link Builder}. */
  public static Builder builder() {
    return new AutoValue_GcsMoveOptions.Builder();
  }

  /** A builder for {@link GcsMoveOptions}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDestinationKmsKeyName(String destinationKmsKeyName);

    public abstract GcsMoveOptions build();
  }
}
