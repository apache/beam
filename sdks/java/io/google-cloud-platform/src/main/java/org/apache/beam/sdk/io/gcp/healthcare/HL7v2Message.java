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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.healthcare.v1beta1.model.Message;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/** The type HL7v2 message to wrap the {@link Message} model. */
public class HL7v2Message {
  private static final String schematizedDataKey = "schematizedData";
  private static final String schematizedDataPrefix = "{data=";
  private final String name;
  private final String messageType;
  private final String sendTime;
  private final String createTime;
  private final String data;
  private final String sendFacility;
  private String schematizedData;
  private final Map<String, String> labels;

  private static String extractDataJson(String schematizedData) {
    String jsonData;
    final ObjectMapper mapper = new ObjectMapper();
    if (schematizedData != null
        && schematizedData.startsWith(schematizedDataPrefix)
        && schematizedData.endsWith("}}")) {
      jsonData =
          schematizedData.substring(schematizedDataPrefix.length(), schematizedData.length() - 1);
    } else {
      jsonData = schematizedData;
    }
    try {
      mapper.readTree(jsonData);
      return jsonData;
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("Could not validate inner schematizedData JSON: %s", e.getMessage()));
    }
  }

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (IOException e) {
      return this.getData();
    }
  }

  /**
   * From model {@link Message} to hl7v2 message.
   *
   * @param msg the model msg
   * @return the hl7v2 message
   */
  public static HL7v2Message fromModel(Message msg) {
    final String schematizedData;
    if (msg.get(schematizedDataKey) != null) {
      schematizedData = extractDataJson(msg.get(schematizedDataKey).toString());
    } else {
      schematizedData = null;
    }
    return new HL7v2Message(
        msg.getName(),
        msg.getMessageType(),
        msg.getSendTime(),
        msg.getCreateTime(),
        msg.getData(),
        msg.getSendFacility(),
        schematizedData,
        msg.getLabels());
  }

  public static HL7v2Message fromMap(Map msg) {
    final String schematizedData;
    if (msg.get(schematizedDataKey) != null) {
      schematizedData = extractDataJson(msg.get(schematizedDataKey).toString());
    } else {
      schematizedData = null;
    }

    return new HL7v2Message(
        msg.get("name").toString(),
        msg.get("messageType").toString(),
        null,
        msg.get("createTime").toString(),
        msg.get("data").toString(),
        msg.get("sendFacility").toString(),
        schematizedData,
        null);
  }
  /**
   * To model message.
   *
   * @return the message
   */
  public Message toModel() {
    Message out = new Message();
    out.setName(this.getName());
    out.setMessageType(this.getMessageType());
    out.setSendTime(this.getSendTime());
    out.setCreateTime(this.getCreateTime());
    out.setData(this.getData());
    out.setSendFacility(this.getSendFacility());
    out.set(schematizedDataKey, this.getSchematizedData());
    out.setLabels(this.labels);
    return out;
  }

  public HL7v2Message(
      String name,
      String messageType,
      String sendTime,
      String createTime,
      String data,
      String sendFacility,
      @Nullable String schematizedData,
      @Nullable Map<String, String> labels) {
    this.name = name;
    this.messageType = messageType;
    this.sendTime = sendTime;
    this.createTime = createTime;
    this.data = data;
    this.sendFacility = sendFacility;
    this.schematizedData = schematizedData;
    this.labels = labels;
  }

  /**
   * Gets name.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Gets message type.
   *
   * @return the message type
   */
  public String getMessageType() {
    return messageType;
  }

  /**
   * Gets send time.
   *
   * @return the send time
   */
  public String getSendTime() {
    return sendTime;
  }

  /**
   * Gets create time.
   *
   * @return the create time
   */
  public String getCreateTime() {
    return createTime;
  }

  /**
   * Gets data.
   *
   * @return the data
   */
  public String getData() {
    return data;
  }

  /**
   * Gets send facility.
   *
   * @return the send facility
   */
  public String getSendFacility() {
    return sendFacility;
  }

  /**
   * Gets schematized data.
   *
   * @return the schematized data
   */
  public String getSchematizedData() {
    return schematizedData;
  }

  public void setSchematizedData(String schematizedData) {
    this.schematizedData = schematizedData;
  }
  /**
   * Gets labels.
   *
   * @return the labels
   */
  public Map<String, String> getLabels() {
    return labels;
  }
}
