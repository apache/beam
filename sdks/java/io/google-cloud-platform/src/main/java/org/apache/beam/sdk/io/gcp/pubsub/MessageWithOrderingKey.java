package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.coders.DefaultCoder;

@AutoValue
@DefaultCoder(MessageWithOrderingKeyCoder.class)
public abstract class MessageWithOrderingKey {
  public abstract PubsubMessage message();
  public abstract String orderingKey();

  public static MessageWithOrderingKey of(PubsubMessage message, String orderingKey) {
    return new AutoValue_MessageWithOrderingKey(message, orderingKey);
  }

  public static MessageWithOrderingKey of(com.google.pubsub.v1.PubsubMessage message) {
    return of(PubsubMessage.of(message), message.getOrderingKey());
  }

  public com.google.pubsub.v1.PubsubMessage toProto() {
    com.google.pubsub.v1.PubsubMessage.Builder builder = com.google.pubsub.v1.PubsubMessage.newBuilder();
    builder.setData(ByteString.copyFrom(message().getPayload()));
    builder.putAllAttributes(message().getAttributeMap());
    if (null != message().getMessageId()) {
      builder.setMessageId(message().getMessageId());
    }
    builder.setOrderingKey(orderingKey());
    return builder.build();
  }
}
