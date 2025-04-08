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
package org.apache.beam.runners.dataflow.worker.windmill.client;

import com.google.auto.value.AutoValue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * Time to live for a {@link WindmillStream}. This should be less than a stream's deadline since we
 * use this to internally restart the stream before we will get a {@link
 * org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Status#DEADLINE_EXCEEDED} errors.
 */
@Internal
@AutoValue
public abstract class WindmillStreamTTL {

  private static final WindmillStreamTTL NO_TTL = createInternal(-1, TimeUnit.SECONDS);

  public static WindmillStreamTTL create(int time, TimeUnit unit) {
    Preconditions.checkState(time > 0, "TTL time must be greater than 0.");
    return createInternal(time, unit);
  }

  private static WindmillStreamTTL createInternal(int time, TimeUnit unit) {
    return new AutoValue_WindmillStreamTTL(time, unit);
  }

  public static WindmillStreamTTL noTtl() {
    return NO_TTL;
  }

  public abstract int time();

  public abstract TimeUnit unit();

  final boolean isValidTtl() {
    return time() > 0;
  }
}
