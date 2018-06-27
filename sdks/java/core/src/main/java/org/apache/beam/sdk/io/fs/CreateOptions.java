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

/** An abstract class that contains common configuration options for creating resources. */
public abstract class CreateOptions {
  /** The file-like resource mime type. */
  public abstract String mimeType();

  /** An abstract builder for {@link CreateOptions}. */
  public abstract static class Builder<BuilderT extends CreateOptions.Builder<BuilderT>> {
    public abstract BuilderT setMimeType(String value);
  }

  /** A standard configuration options with builder. */
  @AutoValue
  public abstract static class StandardCreateOptions extends CreateOptions {

    /** Returns a {@link StandardCreateOptions.Builder}. */
    public static StandardCreateOptions.Builder builder() {
      return new AutoValue_CreateOptions_StandardCreateOptions.Builder();
    }

    /** Builder for {@link StandardCreateOptions}. */
    @AutoValue.Builder
    public abstract static class Builder
        extends CreateOptions.Builder<StandardCreateOptions.Builder> {
      public abstract StandardCreateOptions build();
    }
  }
}
