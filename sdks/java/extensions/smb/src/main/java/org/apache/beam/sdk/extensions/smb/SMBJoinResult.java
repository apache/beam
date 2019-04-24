package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.values.TupleTag;

public class SMBJoinResult implements Serializable {
  private Map<TupleTag, Iterator<?>> valueMap;

  SMBJoinResult(Map<TupleTag, Iterator<?>> valueMap) {
    this.valueMap = valueMap;
  }

  Map<TupleTag, Iterator<?>> getValueMap() {
    return valueMap;
  }

  <V> Iterator<V> getValuesForTag(TupleTag<V> tag) {
    return (Iterator<V>) valueMap.get(tag);
  }

  // @Todo.
  static class SMBJoinResultCoder extends CustomCoder<SMBJoinResult> {
    @Override
    public void encode(SMBJoinResult value, OutputStream outStream)
        throws CoderException, IOException {

    }

    @Override
    public SMBJoinResult decode(InputStream inStream) throws CoderException, IOException {
      return new SMBJoinResult(new HashMap<>());
    }
  }
}
