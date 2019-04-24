package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.MimeTypes;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestSortedBucketFile {

  public static class Reader extends SortedBucketFile.Reader<String> {

    private final List<String> data;
    private final Iterator<String> it;

    public Reader(List<String> data) {
      this.data = data;
      this.it = data.iterator();
    }

    @Override
    public Coder<SortedBucketFile.Reader<String>> coder() {
      return null;
    }

    @Override
    public void prepareRead(ReadableByteChannel channel) throws Exception {}

    @Override
    public String read() throws Exception {
      return it.hasNext() ? it.next() : null;
    }

    @Override
    public void finishRead() throws Exception {}
  }

  public static class Writer extends SortedBucketFile.Writer<String> {

    public List<String> getData() {
      return data;
    }

    private final List<String> data = new ArrayList<>();

    @Override
    public String getMimeType() {
      return MimeTypes.TEXT;
    }

    @Override
    public void prepareWrite(WritableByteChannel channel) throws Exception {}

    @Override
    public void write(String value) throws Exception {
      data.add(value);
    }

    @Override
    public void finishWrite() throws Exception {}
  }

}
