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
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;

/**
 * The result of {@link org.apache.beam.sdk.io.FileSystem#match}.
 */
public abstract class MatchResult {

  private MatchResult() {}

  /**
   * Returns a {@link MatchResult} given the {@link Status} and {@link Metadata}.
   */
  public static MatchResult create(Status status, List<Metadata> metadata) {
    return new AutoValue_MatchResult_Success(status, metadata);
  }

  @AutoValue
  abstract static class Success extends MatchResult {
    abstract List<Metadata> getMetadata();

    @Override
    public List<Metadata> metadata() throws IOException {
      return getMetadata();
    }
  }

  /**
   * Returns a {@link MatchResult} given the {@link Status} and {@link IOException}.
   */
  public static MatchResult create(final Status status, final IOException e) {
    return new AutoValue_MatchResult_Failure(status, e);
  }

  @AutoValue
  abstract static class Failure extends MatchResult {
    abstract IOException getException();

    @Override
    public List<Metadata> metadata() throws IOException {
      throw getException();
    }
  }

  /**
   * Returns a {@link MatchResult} with {@link Status#UNKNOWN}.
   */
  public static MatchResult unknown() {
    return new AutoValue_MatchResult_Failure(
        Status.UNKNOWN,
        new IOException("MatchResult status is UNKNOWN, and metadata is not available."));
  }

  /**
   * Status of the {@link MatchResult}.
   */
  public abstract Status status();

  /**
   * {@link Metadata} of matched files. Note that if {@link #status()} is {@link Status#NOT_FOUND},
   * this may either throw a {@link java.io.FileNotFoundException} or return an empty list,
   * depending on the {@link EmptyMatchTreatment} used in the {@link FileSystems#match} call.
   */
  public abstract List<Metadata> metadata() throws IOException;

  /**
   * {@link Metadata} of a matched file.
   */
  @AutoValue
  public abstract static class Metadata implements Serializable {
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
