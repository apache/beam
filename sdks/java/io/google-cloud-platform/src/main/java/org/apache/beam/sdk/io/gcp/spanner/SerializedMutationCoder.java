package org.apache.beam.sdk.io.gcp.spanner;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.VarInt;

class SerializedMutationCoder extends AtomicCoder<SerializedMutation> {

  private static final SerializedMutationCoder INSTANCE = new SerializedMutationCoder();

  public static SerializedMutationCoder of() {
    return INSTANCE;
  }

  private SerializedMutationCoder() {
  }

  @Override
  public void encode(SerializedMutation value, OutputStream out)
      throws CoderException, IOException {
    encodeByteArray(out, value.getTableName().getBytes(StandardCharsets.UTF_8));
    encodeByteArray(out, value.getEncodedKey());
    encodeByteArray(out, value.getMutationGroupBytes());
  }

  @Override
  public SerializedMutation decode(InputStream is)
      throws CoderException, IOException {
    String tableName = new String(readBytes(is), StandardCharsets.UTF_8);
    byte[] encodedKey = readBytes(is);
    byte[] mutationBytes = readBytes(is);
    return SerializedMutation.create(tableName, encodedKey, mutationBytes);
  }

  private void encodeByteArray(OutputStream out, byte[] bytes) throws IOException {
    int length = bytes.length;
    VarInt.encode(length, out);
    out.write(bytes);
  }

  private byte[] readBytes(InputStream is) throws IOException {
    int len = VarInt.decodeInt(is);
    byte[] tmp = new byte[len];
    new DataInputStream(is).readFully(tmp);
    return tmp;
  }


}
