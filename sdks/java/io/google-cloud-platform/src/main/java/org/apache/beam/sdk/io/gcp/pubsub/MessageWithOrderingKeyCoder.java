package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;

public class MessageWithOrderingKeyCoder extends AtomicCoder<MessageWithOrderingKey> {
  private static Coder<PubsubMessage> CODER = ProtoCoder.of(PubsubMessage.class);

  @Override
  public void encode(MessageWithOrderingKey value, OutputStream outStream) throws IOException {
    CODER.encode(value.toProto(), outStream);
  }

  @Override
  public MessageWithOrderingKey decode(InputStream inStream) throws IOException {
    return MessageWithOrderingKey.of(CODER.decode(inStream));
  }
}
