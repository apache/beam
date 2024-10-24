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
package org.apache.beam.io.requestresponse;

import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.util.SerializableUtils;

/**
 * A default {@link SerializableSupplier} of a {@link BackOff} that relies on {@link
 * FluentBackoff#DEFAULT}. Embedding {@link FluentBackoff#DEFAULT} in a lambda failed {@link
 * SerializableUtils#ensureSerializable}.
 */
class DefaultSerializableBackoffSupplier implements SerializableSupplier<BackOff> {

  @Override
  public BackOff get() {
    return FluentBackoff.DEFAULT.backoff();
  }
}
