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
package org.apache.beam.sdk.io.gcp.pubsublite;

import com.google.auto.value.AutoValue;
import com.google.cloud.pubsublite.TopicPath;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Options needed for a Pub/Sub Lite Publisher. */
@AutoValue
public abstract class PublisherOptions implements Serializable {
  private static final long serialVersionUID = 275311613L;

  // Required parameters.
  public abstract TopicPath topicPath();

  // Optional parameters.
  /**
   * A supplier for the publisher to be used. If enabled, does not use the publisher cache.
   *
   * <p>The returned type must be convertible to Publisher<PublishMetadata>, but Object is used to
   * prevent adding an api surface dependency on guava when this is not used.
   */
  public abstract @Nullable SerializableSupplier<Object> publisherSupplier();

  @Override
  public abstract int hashCode();

  public static Builder newBuilder() {
    return new AutoValue_PublisherOptions.Builder();
  }

  public boolean usesCache() {
    return publisherSupplier() == null;
  }

  @AutoValue.Builder
  public abstract static class Builder {
    // Required parameters.
    public abstract Builder setTopicPath(TopicPath path);

    // Optional parameters.
    /**
     * A supplier for the publisher to be used. If enabled, does not use the publisher cache.
     *
     * <p>The returned type must be convertible to Publisher<PublishMetadata>, but Object is used to
     * prevent adding an api surface dependency on guava when this is not used.
     */
    public abstract Builder setPublisherSupplier(SerializableSupplier<Object> stubSupplier);

    public abstract PublisherOptions build();
  }
}
