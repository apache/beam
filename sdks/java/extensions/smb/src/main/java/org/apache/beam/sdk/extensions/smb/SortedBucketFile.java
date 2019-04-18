package org.apache.beam.sdk.extensions.smb;

import java.io.Serializable;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

// read/write individual records sequentially from a sorted bucket file
// Serializable so it can be shipped to DoFns
public abstract class SortedBucketFile<ValueT> implements Serializable {

  public abstract Reader<ValueT> createReader();

  public abstract Writer<ValueT> createWriter();

  public abstract static class Reader<ValueT> {
    public abstract void prepareRead(ReadableByteChannel channel) throws Exception;
    public abstract ValueT read() throws Exception;
    public abstract void finishRead() throws Exception;
  }

  public abstract static class Writer<ValueT> {
    public abstract String getMimeType();
    public abstract void prepareWrite(WritableByteChannel channel) throws Exception;
    public abstract void write(ValueT value) throws Exception;
    public abstract void finishWrite() throws Exception;
  }
}
