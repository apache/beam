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
import com.google.api.services.healthcare.v1beta1.model.ParsedData;
import com.google.api.services.healthcare.v1beta1.model.SchematizedData;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** The type HL7v2 message to wrap the {@link Message} model. */
public class HL7v2Message {
  private String name;
  private String messageType;
  private String sendTime;
  private String createTime;
  private String data;
  private String sendFacility;
  private ParsedData parsedData;
  private String schematizedData;
  private Map<String, String> labels;

  @Override
  public String toString() {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    } catch (IOException e) {
      return this.getData();
    }
  }

  public HL7v2Message() {}
  /**
   * From model {@link Message} to hl7v2 message.
   *
   * @param msg the model msg
   * @return the hl7v2 message
   */
  public static HL7v2Message fromModel(Message msg) {
    HL7v2MessageBuilder mb =
        new HL7v2MessageBuilder(
            msg.getName(),
            msg.getMessageType(),
            msg.getSendTime(),
            msg.getCreateTime(),
            msg.getData(),
            msg.getSendFacility());
    if (msg.getParsedData() != null) {
      mb.setParsedData(msg.getParsedData());
    } else {
      mb.setParsedData(new ParsedData());
    }
    if (msg.getSchematizedData() != null) {
      mb.setSchematizedData(msg.getSchematizedData().getData());
    } else {
      mb.setSchematizedData("empty");
    }
    if (msg.getLabels() != null) {
      mb.setLabels(msg.getLabels());
    } else {
      mb.setLabels(new HashMap<>());
    }

    return mb.build();
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
    if (this.getParsedData() != null) {
      out.setParsedData(this.getParsedData());
    }
    if (this.getSchematizedData() != null) {
      out.setSchematizedData(new SchematizedData().setData(this.schematizedData));
    }
    if (this.getLabels() != null) {
      out.setLabels(this.labels);
    }
    return out;
  }

  public static class HL7v2MessageBuilder {
    private final String name;
    private final String messageType;
    private final String sendTime;
    private final String createTime;
    private final String data;
    private final String sendFacility;

    private ParsedData parsedData;
    private String schematizedData;
    private Map<String, String> labels;

    public HL7v2MessageBuilder(
        String name,
        String messageType,
        String sendTime,
        String createTime,
        String data,
        String sendFacility) {
      this.name = name;
      this.messageType = messageType;
      this.sendTime = sendTime;
      this.createTime = createTime;
      this.data = data;
      this.sendFacility = sendFacility;
    }

    public HL7v2MessageBuilder setParsedData(ParsedData parsedData) {
      this.parsedData = parsedData;
      return this;
    }

    public HL7v2MessageBuilder setSchematizedData(String schematizedData) {
      this.schematizedData = schematizedData;
      return this;
    }

    public HL7v2MessageBuilder setLabels(Map<String, String> labels) {
      this.labels = labels;
      return this;
    }

    public HL7v2Message build() {
      // call the private constructor in the outer class
      return new HL7v2Message(this);
    }
  }

  public HL7v2Message(HL7v2MessageBuilder mb) {
    this.name = mb.name;
    this.messageType = mb.messageType;
    this.sendTime = mb.sendTime;
    this.createTime = mb.createTime;
    this.data = mb.data;
    this.sendFacility = mb.sendFacility;
    if (mb.parsedData != null) {
      this.parsedData = mb.parsedData;
    }
    if (mb.schematizedData != null) {
      this.schematizedData = mb.schematizedData;
    }
    if (mb.labels != null) {
      this.labels = mb.labels;
    }
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
   * Gets parsed data.
   *
   * @return the parsed data
   */
  public ParsedData getParsedData() {
    return parsedData;
  }

  public void setParsedData(ParsedData parsedData) {
    this.parsedData = parsedData;
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
