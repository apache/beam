package org.apache.beam.sdk.coders;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.ValueBinder;
import com.google.cloud.spanner.Timestamp;
import com.google.cloud.spanner.Date;

import com.google.cloud.ByteArray;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataOutput;
import java.io.DataInput;
import java.util.Map;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;

public class SpannerMutationCoder extends AtomicCoder<Mutation> {

  @JsonCreator
  public static SpannerMutationCoder of() {
    return INSTANCE;
  }

  /***************************/

  private static final SpannerMutationCoder INSTANCE = new SpannerMutationCoder();

  private SpannerMutationCoder() {}

  @Override
  public void encode(Mutation value, OutputStream outStream, Context context) throws IOException, CoderException {
      if (value == null) {
          throw new CoderException("cannot encode a null Mutation");
      }

      if (Mutation.Op.DELETE == value.getOperation())
          throw new UnsupportedOperationException("DELETE Mutations not supported!");

      ByteArrayDataOutput out = ByteStreams.newDataOutput();

      out.writeUTF(value.getOperation().name());
      out.writeUTF(value.getTable());

      Map<String, Value> state = value.asMap();

      // Write number of columns
      out.writeInt(state.size());
    
      // Write out column names, types and values
      ValueSerializer ser = ValueSerializer.of();
      for (String columnName : state.keySet()) {
          Value v = state.get(columnName);
          out.writeUTF(columnName);
          out.writeUTF(v.getType().getCode().name());
          ser.writeTo(out, v);
      }

      byte[] buf = out.toByteArray();
      outStream.write(java.nio.ByteBuffer.allocate(4).putInt(buf.length).array());
      outStream.write(buf);
      outStream.flush();
  }

  @Override
  public Mutation decode(InputStream inStream, Context context) throws IOException {

      byte[] lengthSize = new byte[4];
      inStream.read(lengthSize, 0, 4);
      int expectedSize = java.nio.ByteBuffer.wrap(lengthSize).getInt();
      byte[] data = new byte[expectedSize];
      inStream.read(data, 0, expectedSize);
      ByteArrayDataInput in = ByteStreams.newDataInput(data);

      Mutation.Op operation = Enum.valueOf(Mutation.Op.class, in.readUTF());
      String tableName = in.readUTF();
      Mutation.WriteBuilder builder = makeBuilder(operation, tableName);
      int columnCount = in.readInt();
      ValueDeserializer ser = ValueDeserializer.of();
      // Deserialize column values
      for (int i = 0; i < columnCount; i++) {
          String columnName = in.readUTF();
          builder = ser.readFrom(in, builder.set(columnName));
      }

      return builder.build();
  }

  private Mutation.WriteBuilder makeBuilder(Mutation.Op operation, String tableName) {
      switch(operation) {
          case INSERT:
              return Mutation.newInsertBuilder(tableName);
          case UPDATE:
              return Mutation.newUpdateBuilder(tableName);
          case REPLACE:
              return Mutation.newReplaceBuilder(tableName);
          case INSERT_OR_UPDATE:
              return Mutation.newInsertOrUpdateBuilder(tableName);
      }
      throw new UnsupportedOperationException("Cannot determinate mutation operation or operation unsupported.");
  }

  @Override
  protected long getEncodedElementByteSize(Mutation value, Context context)
      throws Exception {
    //return value.getSerializedSize();
    return 0L;    //TODO: Implement this.
  }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always.
   *         A Spanner table can hold arbitrary {@link Object} instances, which
   *         makes the encoding non-deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "Spanner encodings can hold arbitrary Object instances");
  }


  static class ValueSerializer implements java.io.Serializable {

      private static final ValueSerializer INSTANCE = new ValueSerializer();

      public static ValueSerializer of() {
          return INSTANCE;
      }

      public void writeTo(DataOutput out, Value v) throws IOException {
          if (v.isNull()) {
              out.writeByte(0);  //NULL indicator byte
              return;
          }
          out.writeByte(1);  // Not a null value
          Type.Code c = v.getType().getCode();
          switch (c) {
              case BOOL:
                  out.writeBoolean(v.getBool());
                  break;
              case INT64:
                  out.writeLong(v.getInt64());
                  break;
              case FLOAT64:
                  out.writeDouble(v.getFloat64());
                  break;
              case STRING:
                  out.writeUTF(v.getString());
                  break;
              case BYTES:
                  byte[] b = v.getBytes().toByteArray();
                  out.writeInt(b.length);
                  out.write(b);
                  break;
              case TIMESTAMP:
                  out.writeUTF(v.getTimestamp().toString());
                  break;
              case DATE:
                  out.writeUTF(v.getDate().toString());
                  break;
              case ARRAY:
                  throw new UnsupportedOperationException("ARRAY type not implemented yet.");
              case STRUCT:
                  throw new UnsupportedOperationException("STRUCT type not implemented yet.");
          }
      }
  }
   
  static class ValueDeserializer implements java.io.Serializable {

      private static final ValueDeserializer INSTANCE = new ValueDeserializer();

      public static ValueDeserializer of() {
          return INSTANCE;
      }

      public Mutation.WriteBuilder readFrom(DataInput in, ValueBinder<Mutation.WriteBuilder>  vb) throws IOException {
          Type.Code c = Enum.valueOf(Type.Code.class, in.readUTF());
          byte b = in.readByte();   // NULL indicator
          switch (c) {
              case BOOL:
                  return b == 1 ? vb.to(in.readBoolean()) : vb.to((Boolean) null);
              case INT64:
                  return b == 1 ? vb.to(in.readLong()) : vb.to((Long) null);
              case FLOAT64:
                  return b == 1 ? vb.to(in.readDouble()) : vb.to((Double) null);
              case STRING:
                  return b == 1 ? vb.to(in.readUTF()) : vb.to((String) null);
              case BYTES:
                  if (b == 0)
                      return vb.to((ByteArray) null);
                  int size = in.readInt();
                  byte[] buf = new byte[size];
                  in.readFully(buf);
                  return vb.to(ByteArray.copyFrom(buf));
              case TIMESTAMP:
                  return b == 1 ? vb.to(Timestamp.parseTimestamp(in.readUTF())) : vb.to((Timestamp) null);
              case DATE:
                  return b == 1 ? vb.to(Date.parseDate(in.readUTF())) : vb.to((Date) null);
              case ARRAY:
                  throw new UnsupportedOperationException("ARRAY type not implemented yet.");
              case STRUCT:
                  throw new UnsupportedOperationException("STRUCT type not implemented yet.");
          }
          throw new UnsupportedOperationException("Cannot determine type from input stream or type unsupported.");
      }
  }

}
