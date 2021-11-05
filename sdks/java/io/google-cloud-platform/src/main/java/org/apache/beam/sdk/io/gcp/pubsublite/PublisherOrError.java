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

import com.google.auto.value.AutoOneOf;
import com.google.cloud.pubsublite.MessageMetadata;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.Publisher;

/** A helper representing either a Publisher or an error. */
@AutoOneOf(PublisherOrError.Kind.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
abstract class PublisherOrError {
  enum Kind {
    PUBLISHER,
    ERROR
  }

  abstract Kind getKind();

  abstract Publisher<MessageMetadata> publisher();

  abstract CheckedApiException error();

  static PublisherOrError ofPublisher(Publisher<MessageMetadata> p) {
    return AutoOneOf_PublisherOrError.publisher(p);
  }

  static PublisherOrError ofError(CheckedApiException e) {
    return AutoOneOf_PublisherOrError.error(e);
  }
}
