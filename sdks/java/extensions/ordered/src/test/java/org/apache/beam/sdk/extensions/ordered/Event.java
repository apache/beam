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
package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/**
 * Event class to be used in testing.
 *
 * <p>The event simulate a string being emitted for a particular key, e.g., sensor id or customer
 * id.
 */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class Event implements Serializable {

  public static Event create(long sequence, String groupId, String value) {
    return new AutoValue_Event(sequence, groupId, value);
  }

  /** @return event sequence number */
  public abstract long getSequence();

  /** @return the group id event is associated with */
  public abstract String getKey();

  /** @return value of the event */
  public abstract String getValue();
}
