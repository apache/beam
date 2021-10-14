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
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.Serializable;
import org.joda.time.Duration;

@AutoValue
public abstract class SubscriberOptions implements Serializable {
  private static final long serialVersionUID = 269598118L;

  private static final long MEBIBYTE = 1L << 20;

  private static final Duration MIN_BUNDLE_TIMEOUT = Duration.standardMinutes(1);

  public static final FlowControlSettings DEFAULT_FLOW_CONTROL =
      FlowControlSettings.builder()
          .setMessagesOutstanding(Long.MAX_VALUE)
          .setBytesOutstanding(100 * MEBIBYTE)
          .build();

  // Required parameters.
  public abstract SubscriptionPath subscriptionPath();

  // Optional parameters.
  /** Per-partition flow control parameters for this subscription. */
  public abstract FlowControlSettings flowControlSettings();

  /**
   * The minimum wall time to pass before allowing bundle closure.
   *
   * <p>Setting this to too small of a value will result in increased compute costs and lower
   * throughput per byte. Immediate timeouts (Duration.ZERO) may be useful for testing.
   */
  public abstract Duration minBundleTimeout();

  public static Builder newBuilder() {
    Builder builder = new AutoValue_SubscriberOptions.Builder();
    return builder
        .setFlowControlSettings(DEFAULT_FLOW_CONTROL)
        .setMinBundleTimeout(MIN_BUNDLE_TIMEOUT);
  }

  public abstract Builder toBuilder();

  @CanIgnoreReturnValue
  @AutoValue.Builder
  public abstract static class Builder {
    // Required parameters.
    public abstract Builder setSubscriptionPath(SubscriptionPath path);

    // Optional parameters
    public abstract Builder setFlowControlSettings(FlowControlSettings flowControlSettings);

    public abstract Builder setMinBundleTimeout(Duration minBundleTimeout);

    public abstract SubscriberOptions build();
  }
}
