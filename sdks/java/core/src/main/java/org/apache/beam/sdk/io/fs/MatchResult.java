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
import java.io.IOException;

/**
 * The result of {@link org.apache.beam.sdk.io.FileSystem#match}.
 */
public abstract class MatchResult {

  private MatchResult() {}

  /**
   * Returns a {@link MatchResult} given the {@link Status} and {@link Metadata}.
   */
  public static MatchResult create(final Status status, final Metadata[] metadata) {
    return new MatchResult() {
      @Override
      public Status status() {
        return status;
      }

      @Override
      public Metadata[] metadata() throws IOException {
        return metadata;
      }
    };
  }

  /**
   * Returns a {@link MatchResult} given the {@link Status} and {@link IOException}.
   */
  public static MatchResult create(final Status status, final IOException e) {
    return new MatchResult() {
      @Override
      public Status status() {
        return status;
      }

      @Override
      public Metadata[] metadata() throws IOException {
        throw e;
      }
    };
  }

  /**
   * Returns a {@link MatchResult} with {@link Status#UNKNOWN}.
   */
  public static MatchResult unknown() {
    return new MatchResult() {
      @Override
      public Status status() {
        return Status.UNKNOWN;
      }

      @Override
      public Metadata[] metadata() throws IOException {
        throw new IOException("MatchResult status is UNKNOWN, and metadata is not available.");
      }
    };
  }

  /**
   * Status of the {@link MatchResult}.
   */
  public abstract Status status();

  /**
   * {@link Metadata} of matched files.
   */
  public abstract Metadata[] metadata() throws IOException;

  /**
   * {@link Metadata} of a matched file.
   */
  @AutoValue
  public abstract static class Metadata {
    public abstract ResourceId resourceId();
    public abstract long sizeBytes();
    public abstract boolean isReadSeekEfficient();

    public static Builder builder() {
      return new AutoValue_MatchResult_Metadata.Builder();
    }

    /**
     * Builder class for {@link Metadata}.
     */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setResourceId(ResourceId value);
      public abstract Builder setSizeBytes(long value);
      public abstract Builder setIsReadSeekEfficient(boolean value);
      public abstract Metadata build();
    }
  }

  /**
   * Status of a {@link MatchResult}.
   */
  public enum Status {
    UNKNOWN,
    OK,
    NOT_FOUND,
    ERROR,
  }
}
