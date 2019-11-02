package org.apache.beam.sdk.io.aws2.sqs;

import java.io.Serializable;

public class MyMessage implements Serializable {
  private String body;
  private String messageId;

  public MyMessage(String body) {
    this.body = body;
  }

  public MyMessage(String body, String messageId) {
    this.body = body;
    this.messageId = messageId;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public void setMessageId(String messageId) {
    this.messageId = messageId;
  }

  public String getBody() {
    return this.body;
  }

  public String getMessageId() {
    return this.messageId;
  }
}
