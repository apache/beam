package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1alpha2.model.Message;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.IOUtils;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class MessageCoder extends CustomCoder<Message> {
  MessageCoder(){}
  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());

  @Override
  public void encode(Message value, OutputStream outStream) throws CoderException, IOException {
    STRING_CODER.encode(value.getData(), outStream);
  }

  @Override
  public Message decode(InputStream inStream) throws CoderException, IOException {
    Message msg = new Message();
    msg.setData(STRING_CODER.decode(inStream));
    return msg;
  }
}
