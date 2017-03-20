package org.apache.beam.sdk.coders;

import com.google.cloud.spanner.Struct;
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
import java.util.List;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteStreams;

public class SpannerStructCoder extends AtomicCoder<Struct> {

  @JsonCreator
  public static SpannerStructCoder of() {
    return INSTANCE;
  }

  /***************************/

  private static final SpannerStructCoder INSTANCE = new SpannerStructCoder();

  private SpannerStructCoder() {}

  @Override
  public void encode(Struct value, OutputStream outStream, Context context) throws IOException, CoderException {
      if (value == null) {
          throw new CoderException("cannot encode a null Struct");
      }

      ByteArrayDataOutput out = ByteStreams.newDataOutput();

      List<Type.StructField> fields = value.getType().getStructFields();

      // Write number of columns
      out.writeInt(value.getColumnCount());
    
      // Write out column names, types and values
      ValueSerializer ser = ValueSerializer.of();
    
      for (Type.StructField f : fields) {
          out.writeUTF(f.getName());
          out.writeUTF(f.getType().getCode().name());
          ser.writeTo(out, value, f.getName(), f.getType().getCode());
      }

      byte[] buf = out.toByteArray();
      out.write(java.nio.ByteBuffer.allocate(4).putInt(buf.length).array());
      outStream.write(buf);
      outStream.flush();
  }

  @Override
  public Struct decode(InputStream inStream, Context context) throws IOException {

      byte[] lengthSize = new byte[4];
      inStream.read(lengthSize, 0, 4);
      int expectedSize = java.nio.ByteBuffer.wrap(lengthSize).getInt();
      byte[] data = new byte[expectedSize];
      inStream.read(data, 0, expectedSize);
      ByteArrayDataInput in = ByteStreams.newDataInput(data);

      Struct.Builder builder = Struct.newBuilder();

      int columnCount = in.readInt();
      ValueDeserializer ser = ValueDeserializer.of();
      // Deserialize column values
      for (int i = 0; i < columnCount; i++) {
          String columnName = in.readUTF();
          builder = ser.readFrom(in, builder.set(columnName));
      }

      return builder.build();
  }

  @Override
  protected long getEncodedElementByteSize(Struct value, Context context)
      throws Exception {
    //return value.getSerializedSize();
    return 0L;    //TODO: Implement this.
  }

  /**
   * {@inheritDoc}
   *
   * @throws NonDeterministicException always.
   *         A Datastore kind can hold arbitrary {@link Object} instances, which
   *         makes the encoding non-deterministic.
   */
  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(this,
        "Datastore encodings can hold arbitrary Object instances");
  }


  static class ValueSerializer implements java.io.Serializable {

      private static final ValueSerializer INSTANCE = new ValueSerializer();

      public static ValueSerializer of() {
          return INSTANCE;
      }

      public void writeTo(DataOutput out, Struct v, String colName, Type.Code c) throws IOException {
          switch (c) {
              case BOOL:
                  out.writeBoolean(v.getBoolean(colName));
                  break;
              case INT64:
                  out.writeLong(v.getLong(colName));
                  break;
              case FLOAT64:
                  out.writeDouble(v.getDouble(colName));
                  break;
              case STRING:
                  out.writeUTF(v.getString(colName));
                  break;
              case BYTES:
                  byte[] b = v.getBytes(colName).toByteArray();
                  out.writeInt(b.length);
                  out.write(b);
                  break;
              case TIMESTAMP:
                  out.writeUTF(v.getTimestamp(colName).toString());
                  break;
              case DATE:
                  out.writeUTF(v.getDate(colName).toString());
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

      public Struct.Builder readFrom(DataInput in, ValueBinder<Struct.Builder>  vb) throws IOException {
          Type.Code c = Enum.valueOf(Type.Code.class, in.readUTF());
          switch (c) {
              case BOOL:
                  return vb.to(in.readBoolean());
              case INT64:
                  return vb.to(in.readLong());
              case FLOAT64:
                  return vb.to(in.readDouble());
              case STRING:
                  return vb.to(in.readUTF());
              case BYTES:
                  int size = in.readInt();
                  byte[] buf = new byte[size];
                  in.readFully(buf);
                  return vb.to(ByteArray.copyFrom(buf));
              case TIMESTAMP:
                  return vb.to(Timestamp.parseTimestamp(in.readUTF()));
              case DATE:
                  return vb.to(Date.parseDate(in.readUTF()));
              case ARRAY:
                  throw new UnsupportedOperationException("ARRAY type not implemented yet.");
              case STRUCT:
                  throw new UnsupportedOperationException("STRUCT type not implemented yet.");
          }
          throw new UnsupportedOperationException("Cannot determine type from input stream or type unsupported.");
      }
  }

}
