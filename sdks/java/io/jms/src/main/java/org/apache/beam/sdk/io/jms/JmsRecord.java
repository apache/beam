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

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import javax.jms.Destination;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * JmsRecord contains message payload of the record as well as metadata (JMS headers and
 * properties).
 */
public class JmsRecord implements Serializable {

  private final @Nullable String jmsMessageID;
  private final long jmsTimestamp;
  private final String jmsCorrelationID;
  private final @Nullable Destination jmsReplyTo;
  private final Destination jmsDestination;
  private final int jmsDeliveryMode;
  private final boolean jmsRedelivered;
  private final String jmsType;
  private final long jmsExpiration;
  private final int jmsPriority;
  private final Map<String, Object> properties;
  private final String text;

  public JmsRecord(
      @Nullable String jmsMessageID,
      long jmsTimestamp,
      String jmsCorrelationID,
      @Nullable Destination jmsReplyTo,
      Destination jmsDestination,
      int jmsDeliveryMode,
      boolean jmsRedelivered,
      String jmsType,
      long jmsExpiration,
      int jmsPriority,
      Map<String, Object> properties,
      String text) {
    this.jmsMessageID = jmsMessageID;
    this.jmsTimestamp = jmsTimestamp;
    this.jmsCorrelationID = jmsCorrelationID;
    this.jmsReplyTo = jmsReplyTo;
    this.jmsDestination = jmsDestination;
    this.jmsDeliveryMode = jmsDeliveryMode;
    this.jmsRedelivered = jmsRedelivered;
    this.jmsType = jmsType;
    this.jmsExpiration = jmsExpiration;
    this.jmsPriority = jmsPriority;
    this.properties = properties;
    this.text = text;
  }

  public String getJmsMessageID() {
    return jmsMessageID;
  }

  public long getJmsTimestamp() {
    return jmsTimestamp;
  }

  public String getJmsCorrelationID() {
    return jmsCorrelationID;
  }

  public @Nullable Destination getJmsReplyTo() {
    return jmsReplyTo;
  }

  public Destination getJmsDestination() {
    return jmsDestination;
  }

  public int getJmsDeliveryMode() {
    return jmsDeliveryMode;
  }

  public boolean getJmsRedelivered() {
    return jmsRedelivered;
  }

  public String getJmsType() {
    return jmsType;
  }

  public long getJmsExpiration() {
    return jmsExpiration;
  }

  public int getJmsPriority() {
    return jmsPriority;
  }

  public Map<String, Object> getProperties() {
    return this.properties;
  }

  public String getPayload() {
    return this.text;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        jmsMessageID,
        jmsTimestamp,
        jmsCorrelationID,
        jmsReplyTo,
        jmsDestination,
        jmsDeliveryMode,
        jmsRedelivered,
        jmsType,
        jmsExpiration,
        jmsPriority,
        properties,
        text);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj instanceof JmsRecord) {
      JmsRecord other = (JmsRecord) obj;
      return jmsDestination.equals(other.jmsDestination)
          && jmsDeliveryMode == other.jmsDeliveryMode
          && jmsRedelivered == other.jmsRedelivered
          && jmsExpiration == other.jmsExpiration
          && jmsPriority == other.jmsPriority
          && properties.equals(other.properties)
          && text.equals(other.text);
    } else {
      return false;
    }
  }
}
