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
package org.apache.beam.sdk.io.jms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;

@AutoValue
public abstract class PublicationRetryPolicy implements Serializable {
  public static final Duration DEFAULT_PUBLICATION_RETRY_DURATION = Duration.standardSeconds(15);

  public abstract @Pure int maxPublicationAttempts();

  public abstract @Pure Duration retryDuration();

  public static PublicationRetryPolicy create(int maxPublicationAttempts, Duration retryDuration) {
    Duration defaultRetryDuration =
        retryDuration == null ? DEFAULT_PUBLICATION_RETRY_DURATION : retryDuration;
    checkArgument(maxPublicationAttempts > 0, "maxPublicationAttempts should be greater than 0");
    checkArgument(
        defaultRetryDuration != null && defaultRetryDuration.isLongerThan(Duration.ZERO),
        "retryDuration should be greater than 0");
    return new AutoValue_PublicationRetryPolicy.Builder()
        .setMaxPublicationAttempts(maxPublicationAttempts)
        .setRetryDuration(defaultRetryDuration)
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract PublicationRetryPolicy.Builder setMaxPublicationAttempts(int maxPublicationAttempts);

    abstract PublicationRetryPolicy.Builder setRetryDuration(Duration retryDuration);

    abstract PublicationRetryPolicy build();
  }
}
